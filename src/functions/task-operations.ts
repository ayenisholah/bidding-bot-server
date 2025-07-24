import { ITask } from '../models/task.model';
import { TaskStateManager } from '../utils/task-state-manager';
import { Wallet } from '../models/wallet.model';
import { decryptPrivateKey } from '../utils/crypto';
import { logger, ErrorCategory } from '../utils/logger';
import { 
  cancelOpenseaRequest, 
  cancelMagicedenRequest, 
  cancelBlurRequest,
  cancelAllRelatedBids 
} from './cancellation';
import { DistributedLockManager } from '../utils/lock';
import { removeTaskFromIndexes } from './task';
import { walletCache } from '../utils/wallet-cache';

interface StopTaskResult {
  success: boolean;
  error?: string;
}

interface CancellationResult {
  cancelledCount: number;
  error?: string;
}

export class TaskOperationsManager {
  private stateManager: TaskStateManager;
  private lockManager: DistributedLockManager;
  private skipStats: { [taskId: string]: { opensea: number; magiceden: number; blur: number } } = {};
  private errorStats: { [taskId: string]: { opensea: number; magiceden: number; blur: number } } = {};
  private warningBids: { [taskId: string]: { opensea: boolean; magiceden: boolean; blur: boolean } } = {};
  private abortedTasks: Set<string> = new Set();

  constructor(stateManager: TaskStateManager, lockManager: DistributedLockManager) {
    this.stateManager = stateManager;
    this.lockManager = lockManager;
  }

  async stopTask(task: ITask, start: boolean, marketplace?: string): Promise<StopTaskResult> {
    const taskId = task._id.toString();
    const lockKey = `task-operation:${taskId}`;
    
    try {
      // Acquire lock for the entire operation
      const result = await this.lockManager.withLock(lockKey, async () => {
        const wallet = await walletCache.getWalletById(task.walletId);
        const WALLET_PRIVATE_KEY = decryptPrivateKey(wallet?.encryptedPrivateKey as string) as string;

        // Initialize stats
        this.resetTaskStats(taskId);

        if (!marketplace || task.selectedMarketplaces.length === 0) {
          // Full task stop
          await this.performFullTaskStop(taskId, task);
        }

        // Initialize or update task status
        const currentState = this.stateManager.getTaskState(taskId);
        if (!currentState) {
          await this.stateManager.initializeTaskState(taskId, 'CANCELLING');
        } else {
          await this.stateManager.transitionState(taskId, 'CANCELLING', (state) => {
            state.cancellationPercentage = 0;
            state.cancellationStartTime = Date.now();
            delete state.nextLoopTime;
            state.waitingTime = 0;
          });
        }

        // Handle marketplace-specific or full cancellation
        if (marketplace) {
          await this.handleMarketplaceCancellation(taskId, marketplace);
        } else {
          await this.handleFullCancellation(taskId);
        }

        return { success: true };
      }, 60); // 60 second timeout for stop operations

      if (!result) {
        throw new Error('Failed to acquire lock for task stop operation');
      }

      return result;
    } catch (error) {
      await logger.logError(ErrorCategory.TASK, 'Failed to stop task', {
        taskId,
        marketplace,
        error: error as Error,
        errorMessage: error instanceof Error ? error.message : String(error)
      });
      
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error'
      };
    }
  }

  async startTask(task: ITask, isLoop: boolean = false): Promise<void> {
    const taskId = task._id.toString();
    const lockKey = `task-operation:${taskId}`;
    
    await this.lockManager.withLock(lockKey, async () => {
      // Remove from aborted tasks if present
      this.abortedTasks.delete(taskId);
      
      // Update task state
      const updatedTask = { ...task, running: true };
      this.stateManager.setActiveTask(taskId, updatedTask);
      
      // Initialize or transition state
      const currentState = this.stateManager.getTaskState(taskId);
      if (!currentState) {
        await this.stateManager.initializeTaskState(taskId, 'STARTED');
      } else {
        await this.stateManager.transitionState(taskId, 'STARTED', (state) => {
          state.runningPercentage = 0;
          state.cancellationPercentage = 0;
          state.processingPercentage = 0;
        });
      }
      
      // Reset marketplace completion tracking
      this.stateManager.resetMarketplaceCompletion(taskId);
      
      // Clear any existing timeouts
      this.stateManager.clearTaskTimeout(taskId);
      this.stateManager.clearCountdownTimer(taskId);
      
      await logger.logInfo(ErrorCategory.TASK, 'Task started', {
        taskId,
        isLoop,
        marketplaces: task.selectedMarketplaces
      });
    }, 30);
  }

  private async performFullTaskStop(taskId: string, task: ITask): Promise<void> {
    // Mark as aborted
    this.abortedTasks.add(taskId);
    
    // Update active task
    const updatedTask = { ...task, running: false };
    this.stateManager.setActiveTask(taskId, updatedTask);
    
    // Remove from indexes
    removeTaskFromIndexes(taskId);
    
    // Clear timers
    this.stateManager.clearCountdownTimer(taskId);
    this.stateManager.clearTaskTimeout(taskId);
  }

  private async handleMarketplaceCancellation(taskId: string, marketplace: string): Promise<void> {
    const marketplaceLower = marketplace.toLowerCase();
    let result: CancellationResult;
    
    switch (marketplaceLower) {
      case 'opensea':
        result = await cancelOpenseaRequest(taskId);
        break;
      case 'magiceden':
        result = await cancelMagicedenRequest(taskId);
        break;
      case 'blur':
        result = await cancelBlurRequest(taskId);
        break;
      default:
        throw new Error(`Unknown marketplace: ${marketplace}`);
    }
    
    if (result.error) {
      await logger.logWarning(ErrorCategory.MARKETPLACE, `Failed to cancel ${marketplace} requests`, {
        taskId,
        marketplace,
        errorMessage: result.error
      });
    } else if (result.cancelledCount === 0) {
      // No jobs to cancel, immediately complete
      await this.stateManager.transitionState(taskId, 'STOPPED', (state) => {
        state.cancellationPercentage = 100;
      });
    }
    
    // Clean up parent job
    const parentJobId = `${taskId}-${marketplaceLower}`.toLowerCase();
    this.stateManager.deleteParentJob(parentJobId);
  }

  private async handleFullCancellation(taskId: string): Promise<void> {
    const results = await Promise.allSettled([
      cancelOpenseaRequest(taskId),
      cancelMagicedenRequest(taskId),
      cancelBlurRequest(taskId)
    ]);
    
    let totalCancelledCount = 0;
    const marketplaceNames = ['opensea', 'magiceden', 'blur'];
    
    results.forEach((result, index) => {
      if (result.status === 'rejected' || (result.status === 'fulfilled' && result.value.error)) {
        const error = result.status === 'rejected' ? result.reason : result.value.error;
        logger.logWarning(ErrorCategory.MARKETPLACE, `Failed to cancel ${marketplaceNames[index]} requests`, {
          taskId,
          marketplace: marketplaceNames[index],
          errorMessage: error
        });
      } else if (result.status === 'fulfilled') {
        totalCancelledCount += result.value.cancelledCount;
      }
    });
    
    // Clean up all parent jobs
    marketplaceNames.forEach(marketplace => {
      const parentJobId = `${taskId}-${marketplace}`.toLowerCase();
      this.stateManager.deleteParentJob(parentJobId);
    });
    
    // If no cancellation jobs, immediately complete
    if (totalCancelledCount === 0) {
      await this.stateManager.transitionState(taskId, 'STOPPED', (state) => {
        state.cancellationPercentage = 100;
      });
      
      // Clean up task completely
      this.stateManager.deleteActiveTask(taskId);
      await this.stateManager.cleanupTask(taskId);
    }
  }

  private resetTaskStats(taskId: string): void {
    this.skipStats[taskId] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
    
    this.errorStats[taskId] = {
      opensea: 0,
      magiceden: 0,
      blur: 0
    };
    
    this.warningBids[taskId] = {
      opensea: false,
      magiceden: false,
      blur: false
    };
  }

  // Getter methods for compatibility
  getSkipStats(): typeof this.skipStats {
    return this.skipStats;
  }

  getErrorStats(): typeof this.errorStats {
    return this.errorStats;
  }

  getWarningBids(): typeof this.warningBids {
    return this.warningBids;
  }

  isTaskAborted(taskId: string): boolean {
    return this.abortedTasks.has(taskId);
  }

  markTaskAsAborted(taskId: string): void {
    this.abortedTasks.add(taskId);
  }

  cleanupAbortedTasks(): void {
    const now = Date.now();
    const CLEANUP_THRESHOLD = 30 * 60 * 1000; // 30 minutes
    
    this.abortedTasks.forEach(taskId => {
      const state = this.stateManager.getTaskState(taskId);
      if (state && state.lastUpdate && (now - state.lastUpdate) > CLEANUP_THRESHOLD) {
        this.abortedTasks.delete(taskId);
        this.stateManager.cleanupTask(taskId);
      }
    });
  }
}