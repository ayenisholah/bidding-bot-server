import { QueueEvents } from 'bullmq';
import { EnhancedTaskStateManager } from './enhanced-task-state-manager';
import { JobIdParser } from './job-id-parser';
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
const BLUE = '\x1b[34m';
const YELLOW = '\x1b[33m';
const RESET = '\x1b[0m';

export class EnhancedQueueEventHandlerManager {
  private stateManager: EnhancedTaskStateManager;
  private queueEvents: QueueEvents;
  private enabledMarketplaces: string[];
  private removedTokens: RemovalData = {};
  private traitsToRemove: { [taskId: string]: any[] } = {};

  constructor(
    stateManager: EnhancedTaskStateManager,
    queueEvents: QueueEvents,
    enabledMarketplaces: string[]
  ) {
    this.stateManager = stateManager;
    this.queueEvents = queueEvents;
    this.enabledMarketplaces = enabledMarketplaces;
    this.initializeHandlers();
  }

  private initializeHandlers(): void {
    this.queueEvents.on('waiting', this.handleJobWaiting.bind(this));
    this.queueEvents.on('active', this.handleJobActive.bind(this));
    this.queueEvents.on('completed', this.handleJobCompleted.bind(this));
    this.queueEvents.on('failed', this.handleJobFailed.bind(this));
    this.queueEvents.on('stalled', this.handleJobStalled.bind(this));
  }

  /**
   * Handle job waiting (queued) event
   */
  private async handleJobWaiting({ jobId }: { jobId: string }): Promise<void> {
    try {
      const parsed = JobIdParser.parseJobId(jobId);
      if (!parsed.isValid) {
        await logger.logError(ErrorCategory.QUEUE, 'Invalid job ID in waiting event', { jobId });
        return;
      }

      console.log(BLUE + `Job ${jobId} is queued and waiting` + RESET);

      // Update state for job queued
      await this.stateManager.handleJobQueued(jobId);

      // If this is a cancellation job, handle it differently
      if (parsed.jobType === 'CANCEL') {
        await this.handleCancellationJobQueued(jobId, parsed);
      }
    } catch (error) {
      await logger.logError(ErrorCategory.QUEUE, 'Error handling waiting job', {
        jobId,
        error: error as Error
      });
    }
  }

  /**
   * Handle job active (running) event
   */
  private async handleJobActive({ jobId }: { jobId: string }): Promise<void> {
    try {
      const parsed = JobIdParser.parseJobId(jobId);
      if (!parsed.isValid) return;

      const taskId = parsed.taskId;
      const state = this.stateManager.getTaskState(taskId);
      if (!state) return;

      console.log(GREEN + `Job ${jobId} is now active` + RESET);

      // Transition to RUNNING if we're in QUEUED state
      if (state.status === 'QUEUED') {
        await this.stateManager.transitionState(taskId, 'RUNNING');
      }

      // Update marketplace progress to running
      if (parsed.marketplace) {
        const marketplace = parsed.marketplace as keyof typeof state.marketplaceProgress;
        await this.stateManager.updateTaskProgress(taskId, {
          marketplaceProgress: {
            ...state.marketplaceProgress,
            [marketplace]: {
              ...state.marketplaceProgress[marketplace],
              running: true
            }
          }
        });
      }
    } catch (error) {
      await logger.logError(ErrorCategory.QUEUE, 'Error handling active job', {
        jobId,
        error: error as Error
      });
    }
  }

  /**
   * Handle job completed event with enhanced tracking
   */
  private async handleJobCompleted({ jobId }: { jobId: string }): Promise<void> {
    try {
      const parsed = JobIdParser.parseJobId(jobId);
      if (!parsed.isValid) {
        await logger.logError(ErrorCategory.QUEUE, 'Invalid job ID in completed event', { jobId });
        return;
      }

      console.log(GREEN + `Job ${jobId} completed successfully` + RESET);

      // Handle cancellation jobs
      if (parsed.jobType === 'CANCEL') {
        await this.handleCancellationJobCompleted(jobId, parsed);
        return;
      }

      // Handle regular bidding jobs
      await this.stateManager.handleJobCompleted(jobId);

      // Check if all jobs for this task are complete
      await this.checkTaskCompletion(parsed.taskId);

    } catch (error) {
      await logger.logError(ErrorCategory.QUEUE, 'Error handling completed job', {
        jobId,
        error: error as Error
      });
    }
  }

  /**
   * Handle job failed event with enhanced tracking
   */
  private async handleJobFailed({ jobId, failedReason }: { jobId: string; failedReason: string }): Promise<void> {
    try {
      const parsed = JobIdParser.parseJobId(jobId);
      if (!parsed.isValid) {
        await logger.logError(ErrorCategory.QUEUE, 'Invalid job ID in failed event', { jobId });
        return;
      }

      console.log(`${YELLOW}Job ${jobId} failed: ${failedReason}${RESET}`);

      // Handle cancellation job failures
      if (parsed.jobType === 'CANCEL') {
        await this.handleCancellationJobFailed(jobId, parsed);
        return;
      }

      // Handle regular bidding job failures
      await this.stateManager.handleJobFailed(jobId);

      // Check if all jobs for this task are complete (including failed ones)
      await this.checkTaskCompletion(parsed.taskId);

    } catch (error) {
      await logger.logError(ErrorCategory.QUEUE, 'Error handling failed job', {
        jobId,
        failedReason,
        error: error as Error
      });
    }
  }

  /**
   * Handle job stalled event
   */
  private async handleJobStalled({ jobId }: { jobId: string }): Promise<void> {
    try {
      console.log(`${YELLOW}Job ${jobId} has stalled${RESET}`);

      // Log stalled job for monitoring
      await logger.logError(ErrorCategory.QUEUE, 'Job stalled', { jobId });

    } catch (error) {
      await logger.logError(ErrorCategory.QUEUE, 'Error handling stalled job', {
        jobId,
        error: error as Error
      });
    }
  }

  /**
   * Handle cancellation job queued
   */
  private async handleCancellationJobQueued(jobId: string, parsed: any): Promise<void> {
    const marketplace = parsed.marketplace?.toLowerCase();

    if (!marketplace) return;

    // This will be handled by the parent job tracking system
    console.log(BLUE + `Cancellation job ${jobId} queued for ${marketplace}` + RESET);
  }

  /**
   * Handle cancellation job completion
   */
  private async handleCancellationJobCompleted(jobId: string, parsed: any): Promise<void> {
    const taskId = parsed.taskId;
    const marketplace = parsed.marketplace?.toLowerCase();

    if (!marketplace) return;

    console.log(GREEN + `Cancellation job ${jobId} completed for ${marketplace}` + RESET);

    // Update cancellation progress
    await this.stateManager.handleCancellationProgress(taskId, marketplace);
  }

  /**
   * Handle cancellation job failure
   */
  private async handleCancellationJobFailed(jobId: string, parsed: any): Promise<void> {
    const taskId = parsed.taskId;
    const marketplace = parsed.marketplace?.toLowerCase();

    if (!marketplace) return;

    console.log(YELLOW + `Cancellation job ${jobId} failed for ${marketplace}` + RESET);

    // Update cancellation progress (failed jobs still count as progress)
    await this.stateManager.handleCancellationProgress(taskId, marketplace);
  }

  /**
   * Check if all jobs for a task are complete and handle transitions
   */
  private async checkTaskCompletion(taskId: string): Promise<void> {
    const state = this.stateManager.getTaskState(taskId);
    const task = this.stateManager.getActiveTasks().get(taskId);

    if (!state || !task || state.status !== 'RUNNING') return;

    // Check if all marketplaces are complete based on running percentage
    if (state.runningPercentage >= 100) {
      console.log(GREEN + `Task ${taskId} completed for all marketplaces` + RESET);
      await this.handleAllMarketplacesComplete(taskId, task);
    }
  }

  /**
   * Handle when all marketplaces complete their jobs
   */
  private async handleAllMarketplacesComplete(taskId: string, task: ITask): Promise<void> {
    try {
      // Get wallet and calculate bid price for cancellation operations
      const wallet = await Wallet.findById(task.walletId);
      const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;
      const { currencySymbol } = await calculateBidPrice(task, 0, "magiceden");

      // Handle trait and token removals
      let totalCancellationJobs = 0;

      if (this.traitsToRemove[taskId]?.length > 0) {
        console.log(`Processing ${this.traitsToRemove[taskId].length} trait removals for task ${taskId}`);
        const traitJobs = await cancelRemovedTraitBids(
          this.traitsToRemove[taskId],
          task._id,
          WALLET_PRIVATE_KEY,
          currencySymbol
        );
        totalCancellationJobs += traitJobs;
      }

      if (this.hasRemovedTokens(taskId)) {
        const removedTokens = this.removedTokens[taskId];
        const tokenCount = (removedTokens.opensea?.length || 0) +
          (removedTokens.magiceden?.length || 0) +
          (removedTokens.blur?.length || 0);

        console.log(`Processing ${tokenCount} token removals for task ${taskId}`);
        const tokenJobs = await cancelRemovedTokenBids(
          {
            opensea: removedTokens.opensea || [],
            magiceden: removedTokens.magiceden || [],
            blur: removedTokens.blur || []
          },
          task._id,
          WALLET_PRIVATE_KEY,
          currencySymbol
        );
        totalCancellationJobs += tokenJobs;
      }

      // Clean up removal data
      delete this.traitsToRemove[taskId];
      delete this.removedTokens[taskId];

      if (totalCancellationJobs > 0) {
        console.log(GREEN + `Scheduled ${totalCancellationJobs} cancellation jobs for task ${taskId}` + RESET);
      }

      // Transition to completed
      await this.stateManager.transitionState(taskId, 'COMPLETED');

      // Wait a bit before scheduling next loop
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Schedule next loop if task is still running
      if (task.running) {
        await this.scheduleNextLoop(taskId, task);
      } else {
        console.log(`${YELLOW}Task ${taskId} is not running, skipping loop scheduling${RESET}`);
      }

    } catch (error) {
      await logger.logError(ErrorCategory.TASK_MANAGEMENT, 'Error handling all marketplaces complete', {
        taskId,
        error: error as Error
      });
    }
  }

  /**
   * Schedule next loop iteration with enhanced state management
   */
  private async scheduleNextLoop(taskId: string, task: ITask): Promise<void> {
    try {
      const loopInterval = getExpiry(task.loopInterval);
      const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
      const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);

      console.log(GREEN + `Scheduling task ${taskId} to run again in ${loopInterval} seconds` + RESET);

      // Transition to scheduled state
      await this.stateManager.transitionState(taskId, 'SCHEDULED', (state) => {
        state.waitingTime = secondsUntilNextLoop;
        state.nextLoopTime = nextLoopTime;
        // Reset progress counters
        state.runningPercentage = 0;
        state.processingPercentage = 0;
        state.queueingPercentage = 0;
        state.queuedJobs = 0;
        state.completedRunningJobs = 0;
        state.failedRunningJobs = 0;
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
          console.log(GREEN + `Starting next loop for task ${taskId}` + RESET);
          await this.stateManager.transitionState(taskId, 'STARTED');
          // The main task execution logic should pick this up and start processing
        }
      }, loopInterval * 1000);

      this.stateManager.setTaskTimeout(taskId, timeoutId);

    } catch (error) {
      await logger.logError(ErrorCategory.TASK_MANAGEMENT, 'Error scheduling next loop', {
        taskId,
        error: error as Error
      });
    }
  }

  /**
   * Start task cancellation process
   */
  async startTaskCancellation(taskId: string, marketplace?: string): Promise<void> {
    const state = this.stateManager.getTaskState(taskId);
    const task = this.stateManager.getActiveTasks().get(taskId);

    if (!state || !task) return;

    console.log(YELLOW + `Starting cancellation for task ${taskId}` + (marketplace ? ` (${marketplace})` : '') + RESET);

    // Get initial order counts before starting cancellation
    const [openSeaCount, magicEdenCount, blurCount] = await Promise.all([
      Order.countDocuments({ taskId, marketplace: 'opensea' }),
      Order.countDocuments({ taskId, marketplace: 'magiceden' }),
      Order.countDocuments({ taskId, marketplace: 'blur' })
    ]);

    const totalInitialOrders = openSeaCount + magicEdenCount + blurCount;
    console.log(YELLOW + `[StartCancellation] Task ${taskId} initial order counts - OpenSea: ${openSeaCount}, MagicEden: ${magicEdenCount}, Blur: ${blurCount}, Total: ${totalInitialOrders}` + RESET);

    // Transition to cancelling state
    await this.stateManager.transitionState(taskId, 'CANCELLING', (state) => {
      state.cancellationPercentage = 0;
      state.cancellationStartTime = Date.now();
      state.initialOrderCounts = {
        opensea: openSeaCount,
        magiceden: magicEdenCount,
        blur: blurCount
      };
    });

    // Get orders to cancel
    const whereClause = marketplace ? {
      taskId: taskId,
      marketplace: marketplace
    } : {
      taskId: taskId
    };

    const orders = await Order.find(whereClause);

    if (orders.length > 0) {
      console.log(`Found ${orders.length} orders to cancel for task ${taskId}`);

      // Start cancellation process
      await cancelAllRelatedBids(task, marketplace);

      // Wait for cancellation jobs to complete and verify
      await this.waitForCancellationCompletion(taskId, marketplace);
    } else {
      // Handle completion based on whether it's marketplace-specific or full cancellation
      if (marketplace) {
        // Marketplace-specific cancellation - don't transition to STOPPED
        // Instead, transition back to previous state and update marketplace progress
        console.log(`${GREEN}Marketplace ${marketplace} cancellation completed for task ${taskId}${RESET}`);

        await this.stateManager.updateTaskProgress(taskId, {
          cancellationPercentage: 100,
          marketplaceProgress: {
            ...state.marketplaceProgress,
            [marketplace as keyof typeof state.marketplaceProgress]: {
              ...state.marketplaceProgress[marketplace as keyof typeof state.marketplaceProgress],
              completed: false,
              running: false,
              totalJobs: 0,
              queuedJobs: 0,
              completedJobs: 0,
              failedJobs: 0
            }
          }
        });

        // Return to RUNNING state if task is still running
        if (task.running) {
          await this.stateManager.transitionState(taskId, 'RUNNING');
        }
      } else {
        // Full task cancellation - transition to STOPPED
        await this.stateManager.transitionState(taskId, 'STOPPED', (state) => {
          state.cancellationPercentage = 100;
          delete state.nextLoopTime;
          state.waitingTime = 0;
        });
      }
    }
  }

  /**
   * Helper to check if there are removed tokens
   */
  private hasRemovedTokens(taskId: string): boolean {
    const tokens = this.removedTokens[taskId];
    if (!tokens) return false;

    return (tokens.opensea?.length || 0) > 0 ||
      (tokens.magiceden?.length || 0) > 0 ||
      (tokens.blur?.length || 0) > 0;
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

  /**
   * Wait for cancellation jobs to complete and verify all orders are cancelled
   */
  private async waitForCancellationCompletion(taskId: string, marketplace?: string): Promise<void> {
    const maxWaitTime = 60000; // 60 seconds max wait
    const checkInterval = 1000; // Check every second
    const startTime = Date.now();

    // Import the verification function
    const { verifyCancellationComplete } = await import('../index');

    while (Date.now() - startTime < maxWaitTime) {
      // Check if all cancellation jobs are complete
      const state = this.stateManager.getTaskState(taskId);
      if (!state || state.status !== 'CANCELLING') break;

      // Verify no orders remain
      const verification = await verifyCancellationComplete(taskId, marketplace);

      if (verification.complete) {
        console.log(GREEN + `All orders cancelled for task ${taskId}${marketplace ? ` (${marketplace})` : ''}` + RESET);

        // Force completion
        await this.handleCancellationComplete(taskId, marketplace);
        return;
      }

      // Check cancellation percentage
      if (state.cancellationPercentage >= 99) {
        // If we're at 99% but orders remain, force check again
        console.log(YELLOW + `Cancellation at ${state.cancellationPercentage}% but ${verification.remainingOrders} orders remain` + RESET);

        if (verification.marketplaceBreakdown) {
          console.log(`Remaining orders - OpenSea: ${verification.marketplaceBreakdown.opensea}, MagicEden: ${verification.marketplaceBreakdown.magiceden}, Blur: ${verification.marketplaceBreakdown.blur}`);
        }
      }

      // Wait before next check
      await new Promise(resolve => setTimeout(resolve, checkInterval));
    }

    // Timeout reached - force completion anyway
    console.log(YELLOW + `Cancellation timeout reached for task ${taskId}, forcing completion` + RESET);
    await this.handleCancellationComplete(taskId, marketplace);
  }

  /**
   * Handle cancellation completion
   */
  private async handleCancellationComplete(taskId: string, marketplace?: string): Promise<void> {
    const state = this.stateManager.getTaskState(taskId);
    const task = this.stateManager.getActiveTasks().get(taskId);

    if (!state || !task) return;

    console.log(GREEN + `[CancellationComplete] Forcing completion for task ${taskId}${marketplace ? ` (${marketplace})` : ''}` + RESET);

    if (marketplace) {
      // Marketplace-specific cancellation completed
      await this.stateManager.updateTaskProgress(taskId, {
        cancellationPercentage: 100,
        marketplaceProgress: {
          ...state.marketplaceProgress,
          [marketplace as keyof typeof state.marketplaceProgress]: {
            ...state.marketplaceProgress[marketplace as keyof typeof state.marketplaceProgress],
            completed: false,
            running: false,
            totalJobs: 0,
            queuedJobs: 0,
            completedJobs: 0,
            failedJobs: 0
          }
        }
      });

      // Return to appropriate state
      if (task.running && state.status === 'CANCELLING') {
        await this.stateManager.transitionState(taskId, 'RUNNING');
      }
    } else {
      // Full task cancellation - force 100% and cleanup
      await this.stateManager.updateTaskProgress(taskId, {
        cancellationPercentage: 100
      });

      // Clean up parent jobs
      const marketplaces = ['OPENSEA', 'MAGICEDEN', 'BLUR'];
      for (const mp of marketplaces) {
        const parentJobId = `${taskId}-CANCEL_${mp}_BID`;
        // Access parentJobs from stateManager
        (this.stateManager as any).parentJobs.delete(parentJobId);
      }

      // Transition to STOPPED
      await this.stateManager.transitionState(taskId, 'STOPPED', (state) => {
        state.cancellationPercentage = 100;
        delete state.nextLoopTime;
        state.waitingTime = 0;
      });
    }
  }
}