import { DistributedLockManager } from './lock';
import { ITask } from '../models/task.model';
import { JobIdParser, ParsedJobId } from './job-id-parser';
import { logger, ErrorCategory } from './logger';

export type EnhancedTaskStatus = 
  | 'STARTED' 
  | 'PROCESSING' 
  | 'QUEUEING' 
  | 'QUEUED' 
  | 'RUNNING' 
  | 'COMPLETED' 
  | 'SCHEDULED' 
  | 'CANCELLING' 
  | 'STOPPED';

export interface EnhancedTaskStatusInfo {
  status: EnhancedTaskStatus;
  runningPercentage: number;
  cancellationPercentage: number;
  processingPercentage: number;
  queueingPercentage: number;
  waitingTime: number;
  nextLoopTime?: Date;
  cancellationStartTime?: number;
  lastUpdate?: number;
  version?: number;
  // Enhanced tracking
  totalJobsToQueue: number;
  queuedJobs: number;
  totalRunningJobs: number;
  completedRunningJobs: number;
  failedRunningJobs: number;
  // Marketplace tracking
  marketplaceProgress: {
    opensea: MarketplaceProgress;
    magiceden: MarketplaceProgress;
    blur: MarketplaceProgress;
  };
}

export interface MarketplaceProgress {
  enabled: boolean;
  processing: boolean;
  queuing: boolean;
  queued: boolean;
  running: boolean;
  completed: boolean;
  totalJobs: number;
  queuedJobs: number;
  completedJobs: number;
  failedJobs: number;
  // MagicEden specific
  batchSize?: number;
  batchCount?: number;
}

export interface EnhancedBulkJobParent {
  totalJobs: number;
  queuedJobs: number;
  completedJobs: number;
  failedJobs: number;
  taskId: string;
  marketplace: string;
  jobType: 'BID' | 'CANCEL';
  // Enhanced for better tracking
  batchSize?: number; // For MagicEden = 500
  expectedOrderCount?: number;
  actualOrderCount?: number;
}

export interface StateTransition {
  from: EnhancedTaskStatus[];
  to: EnhancedTaskStatus;
  condition?: (state: EnhancedTaskStatusInfo) => boolean;
}

// Enhanced state transitions with proper flow
const ENHANCED_STATE_TRANSITIONS: StateTransition[] = [
  // Normal flow: STARTED -> PROCESSING -> QUEUEING -> QUEUED -> RUNNING -> COMPLETED -> SCHEDULED -> STARTED
  { from: ['STARTED'], to: 'PROCESSING' },
  { from: ['PROCESSING'], to: 'QUEUEING' },
  { from: ['QUEUEING'], to: 'QUEUED', condition: (state) => state.queueingPercentage >= 100 },
  { from: ['QUEUED'], to: 'RUNNING' },
  { from: ['RUNNING'], to: 'COMPLETED', condition: (state) => state.runningPercentage >= 100 },
  { from: ['COMPLETED'], to: 'SCHEDULED' },
  { from: ['SCHEDULED'], to: 'STARTED' },
  
  // Cancellation flow from any state
  { from: ['STARTED', 'PROCESSING', 'QUEUEING', 'QUEUED', 'RUNNING', 'SCHEDULED'], to: 'CANCELLING' },
  { from: ['CANCELLING'], to: 'STOPPED', condition: (state) => state.cancellationPercentage >= 100 },
  { from: ['STOPPED'], to: 'STARTED' },
  
  // Error recovery
  { from: ['PROCESSING'], to: 'STARTED' }, // If processing fails
  { from: ['QUEUEING'], to: 'PROCESSING' }, // If queueing fails
  { from: ['QUEUED'], to: 'QUEUEING' }, // If need to requeue
  { from: ['RUNNING'], to: 'QUEUED' }, // If jobs need to be requeued
];

export class EnhancedTaskStateManager {
  private taskStatus: Map<string, EnhancedTaskStatusInfo> = new Map();
  private parentJobs: Map<string, EnhancedBulkJobParent> = new Map();
  private activeTasks: Map<string, ITask> = new Map();
  private taskTimeouts: Map<string, NodeJS.Timeout> = new Map();
  private taskCountdownTimers: Map<string, NodeJS.Timeout> = new Map();
  private lockManager: DistributedLockManager;
  
  // State change listeners
  private stateChangeListeners: Map<string, ((oldState: EnhancedTaskStatus, newState: EnhancedTaskStatus) => void)[]> = new Map();

  // Marketplace configurations
  private readonly MARKETPLACE_CONFIG = {
    magiceden: {
      batchSize: 500,
      name: 'MagicEden'
    },
    opensea: {
      batchSize: 1000, // OpenSea can handle larger batches
      name: 'OpenSea'
    },
    blur: {
      batchSize: 1000, // Blur can handle larger batches
      name: 'Blur'
    }
  };

  constructor(lockManager: DistributedLockManager) {
    this.lockManager = lockManager;
  }

  /**
   * Initialize enhanced task state with proper marketplace tracking
   */
  async initializeEnhancedTaskState(taskId: string, task: ITask, status: EnhancedTaskStatus = 'STARTED'): Promise<void> {
    const lockKey = `enhanced-task-state:${taskId}`;
    
    await this.lockManager.withLock(lockKey, async () => {
      if (!this.taskStatus.has(taskId)) {
        const initialState: EnhancedTaskStatusInfo = {
          status,
          runningPercentage: 0,
          cancellationPercentage: 0,
          processingPercentage: 0,
          queueingPercentage: 0,
          waitingTime: 0,
          lastUpdate: Date.now(),
          version: 1,
          totalJobsToQueue: 0,
          queuedJobs: 0,
          totalRunningJobs: 0,
          completedRunningJobs: 0,
          failedRunningJobs: 0,
          marketplaceProgress: {
            opensea: this.createMarketplaceProgress(task.selectedMarketplaces.includes('OpenSea')),
            magiceden: this.createMarketplaceProgress(task.selectedMarketplaces.includes('MagicEden')),
            blur: this.createMarketplaceProgress(task.selectedMarketplaces.includes('Blur'))
          }
        };
        
        this.taskStatus.set(taskId, initialState);
        this.activeTasks.set(taskId, task);
      }
    }, 30);
  }

  /**
   * Enhanced state transition with better validation
   */
  async transitionState(taskId: string, newStatus: EnhancedTaskStatus, updater?: (info: EnhancedTaskStatusInfo) => void): Promise<boolean> {
    const lockKey = `enhanced-task-state:${taskId}`;
    
    return await this.lockManager.withLock(lockKey, async () => {
      const currentState = this.taskStatus.get(taskId);
      if (!currentState) {
        await logger.logError(ErrorCategory.STATE_MANAGEMENT, `Task ${taskId} has no current state`, { taskId, newStatus });
        return false;
      }

      // Validate state transition
      const validTransition = ENHANCED_STATE_TRANSITIONS.find(t => 
        t.from.includes(currentState.status) && t.to === newStatus
      );

      if (!validTransition) {
        await logger.logError(ErrorCategory.STATE_MANAGEMENT, `Invalid state transition for task ${taskId}: ${currentState.status} -> ${newStatus}`, {
          taskId,
          currentStatus: currentState.status,
          requestedStatus: newStatus
        });
        return false;
      }

      // Check transition condition if any
      if (validTransition.condition && !validTransition.condition(currentState)) {
        await logger.logError(ErrorCategory.STATE_MANAGEMENT, `State transition condition not met for task ${taskId}`, {
          taskId,
          currentStatus: currentState.status,
          requestedStatus: newStatus,
          currentState
        });
        return false;
      }

      const oldStatus = currentState.status;
      
      // Update state with version increment
      const updatedState: EnhancedTaskStatusInfo = {
        ...currentState,
        status: newStatus,
        lastUpdate: Date.now(),
        version: (currentState.version || 0) + 1
      };

      // Apply custom updates if provided
      if (updater) {
        updater(updatedState);
      }

      this.taskStatus.set(taskId, updatedState);

      // Notify listeners
      this.notifyStateChange(taskId, oldStatus, newStatus);

      console.log(`[EnhancedStateManager] Task ${taskId} transitioned from ${oldStatus} to ${newStatus}`);
      return true;
    }, 30) || false;
  }

  /**
   * Start processing phase - analyze task and calculate job requirements
   */
  async startProcessing(taskId: string): Promise<void> {
    await this.transitionState(taskId, 'PROCESSING', (state) => {
      state.processingPercentage = 0;
    });

    const task = this.activeTasks.get(taskId);
    if (!task) return;

    // Calculate expected jobs for each marketplace
    const jobEstimate = await this.calculateJobEstimate(task);
    
    await this.updateTaskProgress(taskId, {
      processingPercentage: 50,
      totalJobsToQueue: jobEstimate.total,
      marketplaceProgress: {
        opensea: { ...this.taskStatus.get(taskId)!.marketplaceProgress.opensea, totalJobs: jobEstimate.opensea },
        magiceden: { ...this.taskStatus.get(taskId)!.marketplaceProgress.magiceden, totalJobs: jobEstimate.magiceden },
        blur: { ...this.taskStatus.get(taskId)!.marketplaceProgress.blur, totalJobs: jobEstimate.blur }
      }
    });

    // Complete processing
    await this.updateTaskProgress(taskId, { processingPercentage: 100 });
    await this.transitionState(taskId, 'QUEUEING');
  }

  /**
   * Start queueing phase - create and queue all jobs
   */
  async startQueueing(taskId: string): Promise<void> {
    const state = this.taskStatus.get(taskId);
    if (!state) return;

    // Update marketplace progress to queuing
    await this.updateTaskProgress(taskId, {
      queueingPercentage: 0,
      marketplaceProgress: {
        opensea: { ...state.marketplaceProgress.opensea, queuing: true },
        magiceden: { ...state.marketplaceProgress.magiceden, queuing: true },
        blur: { ...state.marketplaceProgress.blur, queuing: true }
      }
    });
  }

  /**
   * Handle job queued event with enhanced tracking
   */
  async handleJobQueued(jobId: string): Promise<void> {
    const parsed = JobIdParser.parseJobId(jobId);
    if (!parsed.isValid) {
      await logger.logError(ErrorCategory.QUEUE, 'Invalid job ID format', { jobId });
      return;
    }

    const taskId = parsed.taskId;
    const state = this.taskStatus.get(taskId);
    if (!state) return;

    // Update queued jobs count
    await this.updateTaskProgress(taskId, {
      queuedJobs: state.queuedJobs + 1
    });

    // Update marketplace progress
    if (parsed.marketplace) {
      const marketplace = parsed.marketplace as keyof typeof state.marketplaceProgress;
      await this.updateTaskProgress(taskId, {
        marketplaceProgress: {
          ...state.marketplaceProgress,
          [marketplace]: {
            ...state.marketplaceProgress[marketplace],
            queuedJobs: state.marketplaceProgress[marketplace].queuedJobs + 1
          }
        }
      });
    }

    // Calculate queueing percentage
    const queueingPercentage = state.totalJobsToQueue > 0 ? 
      (state.queuedJobs / state.totalJobsToQueue) * 100 : 0;

    await this.updateTaskProgress(taskId, { queueingPercentage });

    // Transition to QUEUED if all jobs are queued
    if (queueingPercentage >= 100) {
      await this.transitionState(taskId, 'QUEUED');
    }
  }

  /**
   * Handle job completion with enhanced marketplace tracking
   */
  async handleJobCompleted(jobId: string): Promise<void> {
    const parsed = JobIdParser.parseJobId(jobId);
    if (!parsed.isValid) return;

    const taskId = parsed.taskId;
    const state = this.taskStatus.get(taskId);
    if (!state || state.status !== 'RUNNING') return;

    // Update completion counts
    await this.updateTaskProgress(taskId, {
      completedRunningJobs: state.completedRunningJobs + 1
    });

    // Update marketplace progress
    if (parsed.marketplace) {
      const marketplace = parsed.marketplace as keyof typeof state.marketplaceProgress;
      await this.updateTaskProgress(taskId, {
        marketplaceProgress: {
          ...state.marketplaceProgress,
          [marketplace]: {
            ...state.marketplaceProgress[marketplace],
            completedJobs: state.marketplaceProgress[marketplace].completedJobs + 1
          }
        }
      });
    }

    // Update parent job if exists
    await this.updateParentJob(jobId, parsed, { completed: 1 });

    // Recalculate running percentage
    await this.recalculateRunningPercentage(taskId);
  }

  /**
   * Handle job failure with enhanced tracking
   */
  async handleJobFailed(jobId: string): Promise<void> {
    const parsed = JobIdParser.parseJobId(jobId);
    if (!parsed.isValid) return;

    const taskId = parsed.taskId;
    const state = this.taskStatus.get(taskId);
    if (!state || state.status !== 'RUNNING') return;

    // Update failure counts
    await this.updateTaskProgress(taskId, {
      failedRunningJobs: state.failedRunningJobs + 1
    });

    // Update marketplace progress
    if (parsed.marketplace) {
      const marketplace = parsed.marketplace as keyof typeof state.marketplaceProgress;
      await this.updateTaskProgress(taskId, {
        marketplaceProgress: {
          ...state.marketplaceProgress,
          [marketplace]: {
            ...state.marketplaceProgress[marketplace],
            failedJobs: state.marketplaceProgress[marketplace].failedJobs + 1
          }
        }
      });
    }

    // Update parent job if exists
    await this.updateParentJob(jobId, parsed, { failed: 1 });

    // Recalculate running percentage
    await this.recalculateRunningPercentage(taskId);
  }

  /**
   * Enhanced cancellation handling with marketplace-specific weights
   */
  async handleCancellationProgress(taskId: string, marketplace?: string): Promise<void> {
    const state = this.taskStatus.get(taskId);
    if (!state || state.status !== 'CANCELLING') return;

    let cancellationPercentage: number;

    if (marketplace) {
      // Single marketplace cancellation
      cancellationPercentage = await this.calculateMarketplaceCancellationPercentage(taskId, marketplace);
    } else {
      // Combined cancellation with proper weighting
      cancellationPercentage = await this.calculateWeightedCancellationPercentage(taskId);
    }

    await this.updateTaskProgress(taskId, { cancellationPercentage });

    // Complete cancellation if 100%
    if (cancellationPercentage >= 100) {
      await this.transitionState(taskId, 'STOPPED', (state) => {
        state.cancellationPercentage = 100;
        delete state.nextLoopTime;
        state.waitingTime = 0;
      });
    }
  }

  /**
   * Calculate weighted cancellation percentage considering MagicEden batching
   */
  private async calculateWeightedCancellationPercentage(taskId: string): Promise<number> {
    // Get actual order counts from database
    const { prisma } = await import('./prisma');
    const [openSeaCount, magicEdenCount, blurCount] = await Promise.all([
      prisma.order.count({ where: { taskId, marketplace: 'opensea' } }),
      prisma.order.count({ where: { taskId, marketplace: 'magiceden' } }),
      prisma.order.count({ where: { taskId, marketplace: 'blur' } })
    ]);

    const totalOrderCount = openSeaCount + magicEdenCount + blurCount;
    if (totalOrderCount === 0) return 100;

    let weightedPercentage = 0;

    // Calculate for each marketplace with proper batching consideration
    const marketplaces = [
      { name: 'opensea', count: openSeaCount, batchSize: this.MARKETPLACE_CONFIG.opensea.batchSize },
      { name: 'magiceden', count: magicEdenCount, batchSize: this.MARKETPLACE_CONFIG.magiceden.batchSize },
      { name: 'blur', count: blurCount, batchSize: this.MARKETPLACE_CONFIG.blur.batchSize }
    ];

    for (const { name, count, batchSize } of marketplaces) {
      if (count > 0) {
        const parentJobId = `${taskId}-CANCEL_${name.toUpperCase()}_BID`;
        const parentJob = this.parentJobs.get(parentJobId);
        
        if (parentJob) {
          // For MagicEden, account for batching
          const expectedBatches = Math.ceil(count / batchSize);
          const actualBatches = parentJob.totalJobs;
          
          const progress = parentJob.completedJobs + parentJob.failedJobs;
          const percentage = actualBatches > 0 ? (progress / actualBatches) * 100 : 100;
          
          // Weight by actual order count, not batch count
          weightedPercentage += (count * percentage);
        } else {
          weightedPercentage += (count * 100);
        }
      }
    }

    return Math.min(weightedPercentage / totalOrderCount, 100);
  }

  /**
   * Calculate marketplace-specific cancellation percentage
   */
  private async calculateMarketplaceCancellationPercentage(taskId: string, marketplace: string): Promise<number> {
    const parentJobId = `${taskId}-CANCEL_${marketplace.toUpperCase()}_BID`;
    const parentJob = this.parentJobs.get(parentJobId);
    
    if (!parentJob || parentJob.totalJobs < 1) return 100;
    
    return ((parentJob.completedJobs + parentJob.failedJobs) / parentJob.totalJobs) * 100;
  }

  /**
   * Recalculate running percentage with marketplace consideration
   */
  private async recalculateRunningPercentage(taskId: string): Promise<void> {
    const state = this.taskStatus.get(taskId);
    const task = this.activeTasks.get(taskId);
    if (!state || !task) return;

    const selectedMarketplaces = task.selectedMarketplaces.map(m => m.toLowerCase());
    let totalJobs = 0;
    let completedJobs = 0;
    let failedJobs = 0;

    for (const marketplace of selectedMarketplaces) {
      const progress = state.marketplaceProgress[marketplace as keyof typeof state.marketplaceProgress];
      if (progress.enabled) {
        totalJobs += progress.totalJobs;
        completedJobs += progress.completedJobs;
        failedJobs += progress.failedJobs;
      }
    }

    const runningPercentage = totalJobs > 0 ? ((completedJobs + failedJobs) / totalJobs) * 100 : 0;
    
    await this.updateTaskProgress(taskId, { 
      runningPercentage: Math.min(runningPercentage, 100),
      totalRunningJobs: totalJobs,
      completedRunningJobs: completedJobs,
      failedRunningJobs: failedJobs
    });

    // Check if all marketplaces are complete
    const allComplete = selectedMarketplaces.every(marketplace => {
      const progress = state.marketplaceProgress[marketplace as keyof typeof state.marketplaceProgress];
      return !progress.enabled || (progress.completedJobs + progress.failedJobs >= progress.totalJobs);
    });

    if (allComplete && runningPercentage >= 100) {
      await this.transitionState(taskId, 'COMPLETED');
    }
  }

  /**
   * Update parent job with enhanced tracking
   */
  private async updateParentJob(jobId: string, parsed: ParsedJobId, updates: { completed?: number; failed?: number }): Promise<void> {
    const parentJobId = this.generateParentJobId(parsed);
    if (!parentJobId) return;

    const lockKey = `parent-job:${parentJobId}`;
    
    await this.lockManager.withLock(lockKey, async () => {
      const job = this.parentJobs.get(parentJobId);
      if (!job) return;

      if (updates.completed !== undefined) {
        job.completedJobs += updates.completed;
      }
      if (updates.failed !== undefined) {
        job.failedJobs += updates.failed;
      }

      this.parentJobs.set(parentJobId, job);
    }, 30);
  }

  /**
   * Generate consistent parent job ID
   */
  private generateParentJobId(parsed: ParsedJobId): string | null {
    if (!parsed.marketplace || !parsed.jobType) return null;
    
    const prefix = parsed.jobType === 'CANCEL' ? 'CANCEL_' : '';
    const suffix = parsed.jobType === 'CANCEL' ? '_BID' : '';
    
    return `${parsed.taskId}-${prefix}${parsed.marketplace.toUpperCase()}${suffix}`;
  }

  /**
   * Calculate job estimate for task processing
   */
  private async calculateJobEstimate(task: ITask): Promise<{total: number, opensea: number, magiceden: number, blur: number}> {
    // This would need to be implemented based on task configuration
    // For now, returning placeholder values
    const estimate = {
      opensea: task.selectedMarketplaces.includes('OpenSea') ? 100 : 0,
      magiceden: task.selectedMarketplaces.includes('MagicEden') ? 100 : 0,
      blur: task.selectedMarketplaces.includes('Blur') ? 100 : 0,
      total: 0
    };
    
    estimate.total = estimate.opensea + estimate.magiceden + estimate.blur;
    return estimate;
  }

  /**
   * Create initial marketplace progress
   */
  private createMarketplaceProgress(enabled: boolean): MarketplaceProgress {
    return {
      enabled,
      processing: false,
      queuing: false,
      queued: false,
      running: false,
      completed: false,
      totalJobs: 0,
      queuedJobs: 0,
      completedJobs: 0,
      failedJobs: 0
    };
  }

  /**
   * Update task progress (thread-safe)
   */
  async updateTaskProgress(taskId: string, updates: Partial<EnhancedTaskStatusInfo>): Promise<void> {
    const lockKey = `enhanced-task-state:${taskId}`;
    
    await this.lockManager.withLock(lockKey, async () => {
      const current = this.taskStatus.get(taskId);
      if (current) {
        this.taskStatus.set(taskId, {
          ...current,
          ...updates,
          lastUpdate: Date.now(),
          version: (current.version || 0) + 1
        });
      }
    }, 30);
  }

  /**
   * Get task state
   */
  getTaskState(taskId: string): EnhancedTaskStatusInfo | null {
    return this.taskStatus.get(taskId) || null;
  }

  /**
   * Clean up all resources for a task
   */
  async cleanupTask(taskId: string): Promise<void> {
    const lockKey = `enhanced-task-cleanup:${taskId}`;
    
    await this.lockManager.withLock(lockKey, async () => {
      this.clearTaskTimeout(taskId);
      this.clearCountdownTimer(taskId);
      this.taskStatus.delete(taskId);
      this.activeTasks.delete(taskId);
      this.stateChangeListeners.delete(taskId);
      
      // Clean up parent jobs
      const jobsToDelete: string[] = [];
      this.parentJobs.forEach((job, key) => {
        if (job.taskId === taskId) {
          jobsToDelete.push(key);
        }
      });
      jobsToDelete.forEach(key => this.parentJobs.delete(key));
    }, 30);
  }

  // Timer management methods
  setTaskTimeout(taskId: string, timeout: NodeJS.Timeout): void {
    const existing = this.taskTimeouts.get(taskId);
    if (existing) clearTimeout(existing);
    this.taskTimeouts.set(taskId, timeout);
  }

  clearTaskTimeout(taskId: string): void {
    const timeout = this.taskTimeouts.get(taskId);
    if (timeout) {
      clearTimeout(timeout);
      this.taskTimeouts.delete(taskId);
    }
  }

  setCountdownTimer(taskId: string, timer: NodeJS.Timeout): void {
    const existing = this.taskCountdownTimers.get(taskId);
    if (existing) clearInterval(existing);
    this.taskCountdownTimers.set(taskId, timer);
  }

  clearCountdownTimer(taskId: string): void {
    const timer = this.taskCountdownTimers.get(taskId);
    if (timer) {
      clearInterval(timer);
      this.taskCountdownTimers.delete(taskId);
    }
  }

  // State change notifications
  onStateChange(taskId: string, listener: (oldState: EnhancedTaskStatus, newState: EnhancedTaskStatus) => void): void {
    const listeners = this.stateChangeListeners.get(taskId) || [];
    listeners.push(listener);
    this.stateChangeListeners.set(taskId, listeners);
  }

  private notifyStateChange(taskId: string, oldState: EnhancedTaskStatus, newState: EnhancedTaskStatus): void {
    const listeners = this.stateChangeListeners.get(taskId) || [];
    listeners.forEach(listener => {
      try {
        listener(oldState, newState);
      } catch (error) {
        console.error(`[EnhancedStateManager] Error in state change listener for task ${taskId}:`, error);
      }
    });
  }

  // Getters for compatibility
  getActiveTasks(): Map<string, ITask> {
    return this.activeTasks;
  }

  getParentJobs(): Map<string, EnhancedBulkJobParent> {
    return this.parentJobs;
  }

  getAllTaskStatus(): Map<string, EnhancedTaskStatusInfo> {
    return this.taskStatus;
  }

  // Setters
  setActiveTask(taskId: string, task: ITask): void {
    this.activeTasks.set(taskId, task);
  }

  setParentJob(parentId: string, job: EnhancedBulkJobParent): void {
    this.parentJobs.set(parentId, job);
  }
}