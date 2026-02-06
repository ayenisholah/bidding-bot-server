import { ITask } from '../models/task.model';
import { debugLog, errorLog, infoLog, warnLog } from '../utils/debug-file-logger';

/**
 * BiddingLoopService - Manages bidding loop execution and scheduling
 *
 * Responsibilities:
 * - Core marketplace bid loop execution (timer-based, not queued)
 * - Loop scheduling with retry logic and exponential backoff
 * - Timer management (execution timers + countdown timers)
 * - Loop interval calculation with jitter
 * - State persistence for watchdog recovery
 * - Safety checks to prevent execution for cancelled tasks
 */
class BiddingLoopService {
  // Timer maps
  private taskTimeouts: Map<string, NodeJS.Timeout> | null = null;
  private taskCountdownTimers: Map<string, NodeJS.Timeout> | null = null;
  private timerExecutionIds: Map<string, string> | null = null;

  // Dependencies (injected via initialize)
  private activeTasks: Map<string, ITask> | null = null;
  private taskStatus: any = null;
  private loopCounterStats: any = null;
  private skipStats: any = null;
  private errorStats: any = null;
  private tasksBeingCancelled: Set<string> | null = null;
  private lastScheduleTimes: Map<string, number> | null = null;

  // Helper function dependencies
  private isTaskBlacklisted: ((taskId: string, marketplace: string) => boolean) | null = null;
  private processOpenseaScheduledBid: ((task: ITask) => Promise<void>) | null = null;
  private processBlurScheduledBid: ((task: ITask) => Promise<void>) | null = null;
  private processMagicedenScheduledBid: ((task: ITask) => Promise<void>) | null = null;
  private getExpiry: ((bidDuration: { value: number; unit: string }) => number) | null = null;
  private acquireLoopSchedulingLock: ((taskId: string) => boolean) | null = null;
  private releaseLoopSchedulingLock: ((taskId: string) => void) | null = null;
  private cleanupOldParentJobs: ((taskId: string, currentLoop: number, keepLoops?: number) => void) | null = null;
  private setTaskStatus: ((taskId: string, newStatus: string, context?: string, marketplace?: string) => boolean) | null = null;
  private cleanupTaskResources: ((taskId: string, preserveWarnings?: boolean, preserveParentJobs?: boolean, preserveOrders?: boolean) => void) | null = null;
  private clearTaskTimers: ((taskId: string, marketplace?: string) => void) | null = null;

  // Redis keys removed - loops restart from current time on server restart

  /**
   * Initialize the bidding loop service with dependencies
   */
  public initialize(config: {
    taskTimeouts: Map<string, NodeJS.Timeout>;
    taskCountdownTimers: Map<string, NodeJS.Timeout>;
    timerExecutionIds: Map<string, string>;
    activeTasks: Map<string, ITask>;
    taskStatus: any;
    loopCounterStats: any;
    skipStats: any;
    errorStats: any;
    tasksBeingCancelled: Set<string>;
    lastScheduleTimes: Map<string, number>;
    isTaskBlacklisted: (taskId: string, marketplace: string) => boolean;
    processOpenseaScheduledBid: (task: ITask) => Promise<void>;
    processBlurScheduledBid: (task: ITask) => Promise<void>;
    processMagicedenScheduledBid: (task: ITask) => Promise<void>;
    getExpiry: (bidDuration: { value: number; unit: string }) => number;
    acquireLoopSchedulingLock: (taskId: string) => boolean;
    releaseLoopSchedulingLock: (taskId: string) => void;
    cleanupOldParentJobs: (taskId: string, currentLoop: number, keepLoops?: number) => void;
    setTaskStatus: (taskId: string, newStatus: string, context?: string, marketplace?: string) => boolean;
    cleanupTaskResources: (taskId: string, preserveWarnings?: boolean, preserveParentJobs?: boolean, preserveOrders?: boolean) => void;
    clearTaskTimers: (taskId: string, marketplace?: string) => void;
  }): void {
    // Store timer maps (passed by reference)
    this.taskTimeouts = config.taskTimeouts;
    this.taskCountdownTimers = config.taskCountdownTimers;
    this.timerExecutionIds = config.timerExecutionIds;

    // Store state references
    this.activeTasks = config.activeTasks;
    this.taskStatus = config.taskStatus;
    this.loopCounterStats = config.loopCounterStats;
    this.skipStats = config.skipStats;
    this.errorStats = config.errorStats;
    this.tasksBeingCancelled = config.tasksBeingCancelled;
    this.lastScheduleTimes = config.lastScheduleTimes;

    // Store helper functions
    this.isTaskBlacklisted = config.isTaskBlacklisted;
    this.processOpenseaScheduledBid = config.processOpenseaScheduledBid;
    this.processBlurScheduledBid = config.processBlurScheduledBid;
    this.processMagicedenScheduledBid = config.processMagicedenScheduledBid;
    this.getExpiry = config.getExpiry;
    this.acquireLoopSchedulingLock = config.acquireLoopSchedulingLock;
    this.releaseLoopSchedulingLock = config.releaseLoopSchedulingLock;
    this.cleanupOldParentJobs = config.cleanupOldParentJobs;
    this.setTaskStatus = config.setTaskStatus;
    this.cleanupTaskResources = config.cleanupTaskResources;
    this.clearTaskTimers = config.clearTaskTimers;

    debugLog('STARTUP', 'Bidding loop service initialized');
  }

  /**
   * Execute marketplace bid loop with retry logic
   */
  public async executeMarketplaceBidLoop(taskId: string, marketplace: string): Promise<void> {
    if (!this.activeTasks || !this.isTaskBlacklisted || !this.skipStats || !this.errorStats) {
      throw new Error('BiddingLoopService not initialized');
    }

    const maxRetries = 3;
    const retryDelays = [2000, 5000, 10000]; // 2s, 5s, 10s
    const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
    const startTime = Date.now();

    infoLog('MarketplaceExec', `EXEC_ENTRY | taskId=${taskId} | marketplace=${marketplace} | maxRetries=${maxRetries}`, { taskId, marketplace: mpKey });

    // Reset per-loop stats for this marketplace at the START of each new loop
    // This ensures skipStats and errorStats show per-loop counts, not cumulative across loops
    const _prevSkip = this.skipStats[taskId]?.[mpKey] || 0;
    const _prevError = this.errorStats[taskId]?.[mpKey] || 0;

    // Initialize task-level stats objects if they don't exist
    if (!this.skipStats[taskId]) {
      this.skipStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
    }
    if (!this.errorStats[taskId]) {
      this.errorStats[taskId] = { opensea: 0, magiceden: 0, blur: 0 };
    }

    // Reset the specific marketplace's stats to 0 for this new loop
    this.skipStats[taskId][mpKey] = 0;
    this.errorStats[taskId][mpKey] = 0;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const attemptStart = Date.now();

      try {
        // Validate task state before execution
        const currentTask = this.activeTasks.get(taskId);
        const _isRunning = currentTask?.running === true;
        const isBlacklisted = this.isTaskBlacklisted(taskId, mpKey);

        if (!currentTask?.running) {
          infoLog('MarketplaceExec', `EXEC_ABORT | taskId=${taskId} | marketplace=${marketplace} | reason=task_not_running`, { taskId, marketplace: mpKey });
          return;
        }

        if (isBlacklisted) {
          infoLog('MarketplaceExec', `EXEC_ABORT | taskId=${taskId} | marketplace=${marketplace} | reason=task_blacklisted`, { taskId, marketplace: mpKey });
          return;
        }

        // Execute directly based on marketplace - no BullMQ queue
        switch (mpKey) {
          case 'opensea':
            if (this.processOpenseaScheduledBid) {
              await this.processOpenseaScheduledBid(currentTask);
            }
            break;
          case 'blur':
            if (this.processBlurScheduledBid) {
              await this.processBlurScheduledBid(currentTask);
            }
            break;
          case 'magiceden':
            if (this.processMagicedenScheduledBid) {
              await this.processMagicedenScheduledBid(currentTask);
            }
            break;
          default:
            warnLog('MarketplaceExec', `EXEC_UNKNOWN_MARKETPLACE | taskId=${taskId} | marketplace=${marketplace}`, { taskId, marketplace: mpKey });
            return;
        }

        const attemptDuration = Date.now() - attemptStart;
        const totalDuration = Date.now() - startTime;

        infoLog('MarketplaceExec', `EXEC_SUCCESS | taskId=${taskId} | marketplace=${marketplace} | attempt=${attempt + 1}/${maxRetries} | attemptMs=${attemptDuration} | totalMs=${totalDuration}`, { taskId, marketplace: mpKey });
        return; // Success - exit retry loop

      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        const attemptDuration = Date.now() - attemptStart;

        if (attempt < maxRetries - 1) {
          const delayMs = retryDelays[attempt];
          warnLog('MarketplaceExec', `EXEC_RETRY | taskId=${taskId} | marketplace=${marketplace} | attempt=${attempt + 1}/${maxRetries} | delayMs=${delayMs} | attemptMs=${attemptDuration} | error=${errorMessage}`, { taskId, marketplace: mpKey });
          await new Promise(resolve => setTimeout(resolve, delayMs));
        } else {
          // Final failure - schedule recovery via next loop
          const totalDuration = Date.now() - startTime;
          errorLog('MarketplaceExec', `EXEC_EXHAUSTED | taskId=${taskId} | marketplace=${marketplace} | attempts=${maxRetries} | totalMs=${totalDuration} | error=${errorMessage}`, error);

          // Schedule next loop to recover - this will set a new timer
          const currentTask = this.activeTasks.get(taskId);
          const canRecover = currentTask?.running && !this.isTaskBlacklisted!(taskId, mpKey);
          if (canRecover) {
            infoLog('MarketplaceExec', `EXEC_FALLBACK | taskId=${taskId} | marketplace=${marketplace} | action=scheduleNextLoop`, { taskId, marketplace: mpKey });
            this.scheduleNextLoop(taskId, marketplace);
          } else {
            warnLog('MarketplaceExec', `EXEC_NO_RECOVERY | taskId=${taskId} | marketplace=${marketplace} | reason=${!currentTask?.running ? 'not_running' : 'blacklisted'}`, { taskId, marketplace: mpKey });
          }
        }
      }
    }
  }

  /**
   * Schedule the next loop iteration
   */
  public async scheduleNextLoop(taskId: string, marketplace?: string): Promise<void> {
    if (!this.activeTasks || !this.taskStatus || !this.taskTimeouts || !this.taskCountdownTimers ||
      !this.timerExecutionIds || !this.tasksBeingCancelled || !this.lastScheduleTimes ||
      !this.loopCounterStats || !this.getExpiry ||
      !this.acquireLoopSchedulingLock || !this.releaseLoopSchedulingLock ||
      !this.cleanupOldParentJobs || !this.setTaskStatus || !this.cleanupTaskResources ||
      !this.clearTaskTimers) {
      throw new Error('BiddingLoopService not initialized');
    }

    const timerKey = marketplace ? `${taskId}-${marketplace}` : taskId;
    const mpKeyForLog = marketplace?.toLowerCase() as 'opensea' | 'magiceden' | 'blur' | undefined;

    // FIRST CHECK: tasksBeingCancelled Set (synchronous flag)
    // This MUST be the first check because it's set at the very start of stopTask(),
    // before any async operations. Prevents scheduling new timers for tasks being cancelled.
    if (this.tasksBeingCancelled.has(taskId)) {
      return;
    }

    // Get the task from activeTasks
    const task = this.activeTasks.get(taskId);
    if (!task) {
      warnLog('LoopScheduler',
        `EXIT_NO_TASK | taskId=${taskId} | reason=task_not_in_activeTasks`,
        { taskId, marketplace: mpKeyForLog, timerKey, reason: 'task_not_active' }
      );
      this.lastScheduleTimes.delete(taskId);
      return;
    }

    // EARLY EXIT: Don't schedule if marketplace is being cancelled or already stopped
    // This prevents the race condition where scheduleNextLoop is called by a completing job
    // after stopTask has already started the cancellation process
    if (marketplace) {
      const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
      const mpStatusForSchedule = this.taskStatus[taskId]?.marketplaces?.[mpKey];
      if (mpStatusForSchedule?.status === 'CANCELLING' || mpStatusForSchedule?.status === 'STOPPED') {
        return;
      }
    } else {
      // For all-marketplace scheduling, check if task is being cancelled
      const anyMarketplaceCancelling = task.selectedMarketplaces.some(mp => {
        const mpLower = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
        const mpStatus = this.taskStatus[taskId]?.marketplaces?.[mpLower];
        return mpStatus?.status === 'CANCELLING' || mpStatus?.status === 'STOPPED';
      });
      if (anyMarketplaceCancelling) {
        return;
      }
    }

    // Calculate loopInterval with getExpiry()
    let loopInterval = this.getExpiry(task.loopInterval);

    // Handle immediate rescheduling when loopInterval = 0
    if (loopInterval < 0) {
      loopInterval = 60;
    }

    // Enforce minimum loop interval to prevent rapid restarts
    const MIN_LOOP_INTERVAL = 2; // 2 seconds minimum
    if (loopInterval < MIN_LOOP_INTERVAL) {
      loopInterval = MIN_LOOP_INTERVAL;
    }

    // Read loop counter for debug logging (DO NOT increment here)
    // Counter is initialized to 1 in startTaskInternal() and incremented in timer callback when this schedule fires.
    if (marketplace) {
      // Initialize task counters if they don't exist (for first-time access)
      if (!this.loopCounterStats[taskId]) {
        this.loopCounterStats[taskId] = {};
      }
      if (!this.loopCounterStats[taskId][marketplace]) {
        this.loopCounterStats[taskId][marketplace] = 1;
      }
    }
    // Note: All-marketplaces case doesn't increment per-marketplace counters

    // Set up next loop time and countdown timer
    const nextLoopTime = new Date(Date.now() + loopInterval * 1000);
    const secondsUntilNextLoop = Math.floor((nextLoopTime.getTime() - Date.now()) / 1000);

    // BUG FIX: Validate task is still active BEFORE any state changes
    // This prevents the race condition where status is set to SCHEDULED but timer is never created
    if (!this.activeTasks.has(taskId)) {
      this.lastScheduleTimes.delete(taskId);
      return;
    }

    // FIX: Status-Timer Atomicity Bug
    // Only set waitingTime and nextLoopTime here (needed by countdown timer).
    // DO NOT set status to SCHEDULED yet - that happens AFTER timer is successfully created.
    // This prevents orphaned SCHEDULED status when function exits early (e.g., at secondary exit check).
    if (this.taskStatus[taskId]) {
      if (marketplace) {
        // Set timing info for specific marketplace (status set after timer creation)
        const mpKey = marketplace as 'opensea' | 'magiceden' | 'blur';
        if (this.taskStatus[taskId].marketplaces && this.taskStatus[taskId].marketplaces[mpKey]) {
          this.taskStatus[taskId].marketplaces[mpKey].waitingTime = secondsUntilNextLoop;
          this.taskStatus[taskId].marketplaces[mpKey].nextLoopTime = nextLoopTime;
        }
      } else {
        // Set timing info for all marketplaces (status set after timer creation)
        for (const mp of task.selectedMarketplaces) {
          const mpLower = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
          if (this.taskStatus[taskId].marketplaces && this.taskStatus[taskId].marketplaces[mpLower]) {
            this.taskStatus[taskId].marketplaces[mpLower].waitingTime = secondsUntilNextLoop;
            this.taskStatus[taskId].marketplaces[mpLower].nextLoopTime = nextLoopTime;
          }
        }
      }
    }

    // Clear any existing countdown timer before setting a new one
    const existingCountdownTimer = this.taskCountdownTimers.get(timerKey);
    if (existingCountdownTimer) {
      clearInterval(existingCountdownTimer);
      this.taskCountdownTimers.delete(timerKey);
    }

    // Set up countdown timer
    const countdownTimer = setInterval(() => {
      if (!this.taskStatus![taskId]) {
        clearInterval(countdownTimer);
        this.taskCountdownTimers!.delete(timerKey);
        return;
      }

      // Update countdown for specific marketplace or all marketplaces
      if (marketplace) {
        const mpKey = marketplace as 'opensea' | 'magiceden' | 'blur';
        if (this.taskStatus![taskId].marketplaces && this.taskStatus![taskId].marketplaces[mpKey]) {
          const mpStatus = this.taskStatus![taskId].marketplaces[mpKey];
          if (mpStatus.nextLoopTime) {
            const remainingSeconds = Math.max(0, Math.floor((mpStatus.nextLoopTime.getTime() - Date.now()) / 1000));
            mpStatus.waitingTime = remainingSeconds;

            if (remainingSeconds <= 0) {
              clearInterval(countdownTimer);
              this.taskCountdownTimers!.delete(timerKey);
            }
          }
        }
      } else {
        // Update all marketplaces
        let allComplete = true;
        for (const mp of task.selectedMarketplaces) {
          const mpLower = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
          if (this.taskStatus![taskId].marketplaces && this.taskStatus![taskId].marketplaces[mpLower]) {
            const mpStatus = this.taskStatus![taskId].marketplaces[mpLower];
            if (mpStatus.nextLoopTime) {
              const remainingSeconds = Math.max(0, Math.floor((mpStatus.nextLoopTime.getTime() - Date.now()) / 1000));
              mpStatus.waitingTime = remainingSeconds;
              if (remainingSeconds > 0) {
                allComplete = false;
              }
            }
          }
        }

        if (allComplete) {
          clearInterval(countdownTimer);
          this.taskCountdownTimers!.delete(timerKey);
        }
      }
    }, 1000);

    this.taskCountdownTimers.set(timerKey, countdownTimer);

    // Clear any existing timeout for this task/marketplace combination
    const existingTimeout = this.taskTimeouts.get(timerKey);
    if (existingTimeout) {
      clearTimeout(existingTimeout);
      this.taskTimeouts.delete(timerKey);
    }

    // Secondary safeguard: validate task is still active before creating timeout
    // Primary check is at line ~5109, this catches edge cases during countdown setup
    if (!this.activeTasks.has(taskId)) {
      warnLog('LoopScheduler',
        `SECONDARY_EXIT | taskId=${taskId} | mp=${marketplace || 'all'} | reason=task_deactivated_during_countdown_setup`,
        { taskId, marketplace: mpKeyForLog, timerKey, reason: 'task_deactivated_during_countdown_setup' }
      );
      this.cleanupTaskResources(taskId);
      this.lastScheduleTimes.delete(taskId);
      return;
    }

    // Generate unique execution ID for this timer (cancellation token pattern)
    // This allows us to detect if the timer was cancelled/replaced even if callback is queued in event loop
    const executionId = `${timerKey}-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    this.timerExecutionIds.set(timerKey, executionId);


    try {
      // Schedule the next iteration with setTimeout
      const timeoutId = setTimeout(async () => {
        // GUARD 1: Check if this timer was cancelled (different executionId means new timer was created or timer was cancelled)
        const currentExecutionId = this.timerExecutionIds!.get(timerKey);
        if (currentExecutionId !== executionId) {
          return;
        }

        // GUARD 2: Check if timer still exists in map (may have been cleared by cleanupTaskResources)
        if (!this.taskTimeouts!.has(timerKey)) {
          this.timerExecutionIds!.delete(timerKey);
          return;
        }

        // GUARD 3: Check tasksBeingCancelled Set (synchronous flag set at START of stopTask)
        // This is the DEFINITIVE check that catches ALL cancellation scenarios because:
        // - It's set synchronously at the very start of stopTask()
        // - It's set BEFORE any async operations that could allow timers to slip through
        // - It catches timers created DURING the stopTask flow but BEFORE marketplaceStatus updates
        if (this.tasksBeingCancelled!.has(taskId)) {
          // Clean up all timers for this task
          this.clearTaskTimers!(taskId);
          this.timerExecutionIds!.delete(timerKey);
          return;
        }

        // GUARD 4: Check if marketplace is being cancelled or stopped (status-based check)
        // This prevents the race condition where a timer fires after stopTask has started
        // but before the timer could be cleared (timer was created AFTER cleanupTaskResources ran)
        if (marketplace) {
          const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
          const mpStatusForGuard4 = this.taskStatus![taskId]?.marketplaces?.[mpKey];
          if (mpStatusForGuard4?.status === 'CANCELLING' || mpStatusForGuard4?.status === 'STOPPED') {
            // Clean up this timer since we're stopping
            const timerKeyToClean = `${taskId}-${marketplace}`;
            const existingTimer = this.taskTimeouts!.get(timerKeyToClean);
            if (existingTimer) {
              clearTimeout(existingTimer);
              this.taskTimeouts!.delete(timerKeyToClean);
            }
            this.timerExecutionIds!.delete(timerKey);
            return;
          }
        } else {
          // For all-marketplace timers, check if ANY marketplace is cancelling
          const currentTaskForGuard4 = this.activeTasks!.get(taskId);
          if (currentTaskForGuard4) {
            for (const mp of currentTaskForGuard4.selectedMarketplaces) {
              const mpLower = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
              const mpStatusForGuard4 = this.taskStatus![taskId]?.marketplaces?.[mpLower];
              if (mpStatusForGuard4?.status === 'CANCELLING' || mpStatusForGuard4?.status === 'STOPPED') {
                this.taskTimeouts!.delete(timerKey);
                this.timerExecutionIds!.delete(timerKey);
                return;
              }
            }
          }
        }

        // Remove self from maps immediately to prevent double-execution
        this.taskTimeouts!.delete(timerKey);
        this.timerExecutionIds!.delete(timerKey);

        // FIX: Change status from SCHEDULED to STARTED immediately when timer fires
        // This prevents false "Missing timer for SCHEDULED task" warnings from verifyScheduledTaskTimers()
        // The health check only triggers recovery when status === 'SCHEDULED' && !hasTimer
        // By changing status to STARTED immediately, the condition fails and no false recovery occurs
        if (marketplace) {
          this.setTaskStatus!(taskId, 'STARTED', 'timer fired, starting loop', marketplace);
        } else {
          const timerTask = this.activeTasks!.get(taskId);
          if (timerTask) {
            for (const mp of timerTask.selectedMarketplaces) {
              this.setTaskStatus!(taskId, 'STARTED', 'timer fired, starting loop', mp.toLowerCase());
            }
          }
        }

        // Acquire scheduling lock when timer expires, not before
        // Use marketplace-specific lock key if marketplace is provided
        const lockKey = marketplace ? `${taskId}-${marketplace}` : taskId;
        const lockAcquired = this.acquireLoopSchedulingLock!(lockKey);
        if (!lockAcquired) {
          // BUG FIX: Don't silently fail - reschedule timer to retry
          warnLog('LockOp',
            `TIMER_LOCK_FAILED_RETRY | timerKey=${timerKey} | lockKey=${lockKey} | rescheduling in 5s`,
            { taskId, marketplace: mpKeyForLog, timerKey, lockKey, acquired: false }
          );
          // Reschedule after 5 seconds instead of silently failing
          setTimeout(async () => {
            const currentTask = this.activeTasks!.get(taskId);
            if (currentTask?.running && marketplace) {
              try {
                await this.executeMarketplaceBidLoop(taskId, marketplace);
              } catch (err) {
                errorLog('TaskRecovery', `Rescheduled timer retry failed: ${(err as Error).message}`, err);
              }
            }
          }, 5000);
          return;
        }

        try {
          // Double-check task is still active and running
          const currentTask = this.activeTasks!.get(taskId);
          const currentStatus = this.taskStatus![taskId];

          // Only skip if all marketplaces are STOPPED (not CANCELLING, as user might want to restart)
          const allMarketplacesStopped = currentStatus?.marketplaces &&
            Object.values(currentStatus.marketplaces).every((mp: any) => mp?.status === 'STOPPED');
          if (allMarketplacesStopped) {
            return;
          }

          if (currentTask?.running) {
            // INCREMENT LOOP COUNTER - at the START of each new loop when timer fires
            // This is the single source of truth for loop counting. Counter starts at 1 (initialized in startTaskInternal)
            // and increments by 1 each time the timer fires for a new loop.
            if (marketplace) {
              if (!this.loopCounterStats![taskId]) {
                this.loopCounterStats![taskId] = {};
              }
              const prevCount = this.loopCounterStats![taskId][marketplace] || 0;
              this.loopCounterStats![taskId][marketplace] = prevCount + 1;
              const newLoopCount = this.loopCounterStats![taskId][marketplace];
              this.cleanupOldParentJobs!(taskId, newLoopCount);
            } else {
              // All marketplaces case - increment each marketplace's counter
              for (const mp of currentTask.selectedMarketplaces) {
                const mpLower = mp.toLowerCase();
                if (!this.loopCounterStats![taskId]) {
                  this.loopCounterStats![taskId] = {};
                }
                const prevCount = this.loopCounterStats![taskId][mpLower] || 0;
                this.loopCounterStats![taskId][mpLower] = prevCount + 1;
              }
              const maxLoopCount = Math.max(...currentTask.selectedMarketplaces.map(mp => this.loopCounterStats![taskId][mp.toLowerCase()] || 1));
              this.cleanupOldParentJobs!(taskId, maxLoopCount);
            }

            // Direct timer execution - call processor functions directly without SCHEDULE jobs
            if (marketplace) {
              // Execute specific marketplace that triggered this timer
              await this.executeMarketplaceBidLoop(taskId, marketplace);
            } else {
              // Execute all marketplaces in parallel (legacy behavior)
              const marketplaces = currentTask.selectedMarketplaces.map(m => m.toLowerCase());
              await Promise.all(
                marketplaces.map(mp => this.executeMarketplaceBidLoop(taskId, mp))
              );
            }
          } else {
            warnLog('TimerOp',
              `TIMER_ABORTED | taskId=${taskId} | reason=task_not_running | running=${currentTask?.running}`,
              { taskId, marketplace: mpKeyForLog, reason: 'task_not_running', running: currentTask?.running }
            );
          }
        } catch (error) {
          // Log detailed error information for debugging
          const errorMessage = error instanceof Error ? error.message : String(error);
          const errorStack = error instanceof Error ? error.stack : undefined;

          errorLog('TimerOp',
            `TIMER_ERROR | taskId=${taskId} | mp=${marketplace || 'all'} | error=${errorMessage}`,
            error,
            { taskId, marketplace: mpKeyForLog, errorMessage, errorStack }
          );

        } finally {
          // Always release the lock after execution
          this.releaseLoopSchedulingLock!(lockKey);
        }
      }, loopInterval * 1000);

      this.taskTimeouts.set(timerKey, timeoutId);

      // FIX: Status-Timer Atomicity - NOW set status to SCHEDULED after timer is successfully created
      // This ensures status is only SCHEDULED when a timer actually exists
      if (marketplace) {
        this.setTaskStatus(taskId, 'SCHEDULED', 'next loop scheduled', marketplace);
      } else {
        // Set status for all marketplaces
        for (const mp of task.selectedMarketplaces) {
          const mpLower = mp.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
          this.setTaskStatus(taskId, 'SCHEDULED', 'next loop scheduled', mpLower);
        }
      }

      // TaskFlow logging: Timeout created successfully - using infoLog for visibility
      infoLog('LoopScheduler',
        `TIMER_SCHEDULED | taskId=${taskId} | marketplace=${marketplace || 'all'} | timerKey=${timerKey} | loopInterval=${loopInterval}s | nextFireAt=${nextLoopTime.toISOString()}`,
        { taskId, marketplace: mpKeyForLog, timerKey, loopInterval, nextLoopTime: nextLoopTime.toISOString() }
      );

    } catch (error) {
      // If scheduling fails, clean up resources
      errorLog('LoopScheduler',
        `SCHEDULE_ERROR | taskId=${taskId} | mp=${marketplace || 'all'}`,
        error,
        { taskId, marketplace: mpKeyForLog }
      );
      this.lastScheduleTimes.delete(taskId);
    }
  }
}

// Singleton export
export const biddingLoopService = new BiddingLoopService();
