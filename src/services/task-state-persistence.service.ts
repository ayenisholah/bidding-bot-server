import { LRUCache } from 'lru-cache';
import { debugLog, errorLog } from '../utils/debug-file-logger';
import type { ITask } from '../models/task.model';

/**
 * Task State Persistence Service
 *
 * Manages task state persistence to LRU caches for crash recovery.
 *
 * Responsibilities:
 * - Persisting task state (status, loop counts, schedules) to LRU caches
 * - Restoring task state from LRU caches on server restart (within process lifecycle)
 * - Recreating timers after server restart
 * - Task diagnostic logging for debugging stuck tasks
 *
 * Features:
 * - Automatic state persistence with TTL
 * - Timer recreation with downtime detection
 * - Comprehensive diagnostic snapshots
 * - Graceful error handling
 *
 * Trade-off: State only persists within process lifecycle (lost on hard crash)
 */

const STATE_TTL = 24 * 60 * 60 * 1000; // 24 hours in ms

/**
 * Dependencies required by TaskStatePersistenceService
 */
interface TaskStatePersistenceDependencies {
  taskStatus: Record<string, any>;
  loopCounterStats: Record<string, any>;
  lastScheduleTimes: Map<string, number>;
  activeTasks: Map<string, ITask>;
  taskTimeouts: Map<string, NodeJS.Timeout>;
  taskCountdownTimers: Map<string, NodeJS.Timeout>;
  tasksInCompletionProcess: Set<string>;
  parentJobs: Map<string, any>;
  getParentJobId: (taskId: string, marketplace: string) => string | null;
  startTask: (task: ITask, start: boolean, marketplace?: string, isRestart?: boolean) => Promise<any>;
}

class TaskStatePersistenceService {
  private deps: TaskStatePersistenceDependencies | null = null;

  // LRU caches for different state types
  private taskStatusCache: LRUCache<string, any>;
  private loopCountCache: LRUCache<string, any>;
  private lastScheduleCache: LRUCache<string, number>;
  private nextLoopCache: LRUCache<string, number>;
  private taskStartedCache: LRUCache<string, number>;

  constructor() {
    this.taskStatusCache = new LRUCache<string, any>({
      max: 500,
      ttl: STATE_TTL,
      updateAgeOnGet: true,
    });

    this.loopCountCache = new LRUCache<string, any>({
      max: 500,
      ttl: STATE_TTL,
      updateAgeOnGet: true,
    });

    this.lastScheduleCache = new LRUCache<string, number>({
      max: 500,
      ttl: STATE_TTL,
      updateAgeOnGet: true,
    });

    this.nextLoopCache = new LRUCache<string, number>({
      max: 1500, // 500 tasks × 3 marketplaces
      ttl: STATE_TTL,
      updateAgeOnGet: true,
    });

    this.taskStartedCache = new LRUCache<string, number>({
      max: 500,
      ttl: STATE_TTL,
      updateAgeOnGet: true,
    });
  }

  /**
   * Initialize the service with dependencies
   */
  public initialize(deps: TaskStatePersistenceDependencies): void {
    this.deps = deps;
    debugLog('TaskStatePersistenceService', 'Initialized');
  }

  /**
   * Persist task state to LRU caches for crash recovery.
   * Called after setTaskStatus() to ensure state survives within process lifecycle.
   */
  public async persistTaskState(taskId: string): Promise<void> {
    if (!this.deps) {
      errorLog('TaskStatePersistence', 'Cannot persist state - service not initialized');
      return;
    }

    try {
      const status = this.deps.taskStatus[taskId];
      const loopCount = this.deps.loopCounterStats[taskId];
      const lastSchedule = this.deps.lastScheduleTimes.get(taskId);
      let _persisted = 0;

      // Persist taskStatus
      if (status) {
        this.taskStatusCache.set(taskId, status);
        _persisted++;
      }

      // Persist loopCounterStats
      if (loopCount) {
        this.loopCountCache.set(taskId, loopCount);
        _persisted++;
      }

      // Persist lastScheduleTimes
      if (lastSchedule) {
        this.lastScheduleCache.set(taskId, lastSchedule);
        _persisted++;
      }

      // Persist nextLoopTime for each marketplace
      if (status?.marketplaces) {
        for (const [mp, mpStatus] of Object.entries(status.marketplaces)) {
          if ((mpStatus as any)?.nextLoopTime) {
            this.nextLoopCache.set(`${taskId}:${mp}`, (mpStatus as any).nextLoopTime.getTime());
            _persisted++;
          }
        }
      }
    } catch (err) {
      errorLog('LRUState', `PERSIST_FAILED | taskId=${taskId}`, err, { taskId });
    }
  }

  /**
   * Capture complete task state at critical moments for debugging stuck tasks.
   * This creates a diagnostic snapshot showing ALL state related to a task.
   */
  public logTaskDiagnostic(taskId: string, marketplace: string | undefined, trigger: string): void {
    if (!this.deps) {
      return;
    }

    const timerKey = marketplace ? `${taskId}-${marketplace}` : taskId;
    const mpKey = marketplace?.toLowerCase() as 'opensea' | 'magiceden' | 'blur' | undefined;
    const parentJobId = marketplace ? this.deps.getParentJobId(taskId, marketplace) : null;
    const parentJob = parentJobId ? this.deps.parentJobs.get(parentJobId) : null;

    const diagnostic = {
      trigger,
      taskId,
      marketplace: mpKey,
      // Task state
      isActive: this.deps.activeTasks.has(taskId),
      taskRunning: this.deps.activeTasks.get(taskId)?.running,
      // Timer state
      hasTimeout: this.deps.taskTimeouts.has(timerKey),
      hasCountdown: this.deps.taskCountdownTimers.has(timerKey),
      // Status state
      status: mpKey ? this.deps.taskStatus[taskId]?.marketplaces?.[mpKey]?.status : 'N/A',
      waitingTime: mpKey ? this.deps.taskStatus[taskId]?.marketplaces?.[mpKey]?.waitingTime : 'N/A',
      runningPercentage: mpKey ? this.deps.taskStatus[taskId]?.marketplaces?.[mpKey]?.runningPercentage : 'N/A',
      // Parent job state
      parentJobId,
      parentJobExists: !!parentJob,
      parentJobTotal: parentJob?.totalJobs,
      parentJobCompleted: parentJob?.completedJobs,
      parentJobAllQueued: parentJob?.allJobsQueued,
      // Locks
      inCompletionProcess: this.deps.tasksInCompletionProcess.has(timerKey),
      loopCount: this.deps.loopCounterStats[taskId]?.[marketplace || ''] || 0
    };

    debugLog('TaskFlow',
      `DIAGNOSTIC | ${trigger} | taskId=${taskId} | mp=${marketplace || 'all'}`,
      diagnostic
    );
  }

  /**
   * Persist task start timestamp to LRU cache.
   * Called when a task first starts.
   */
  public async persistTaskStartTime(taskId: string): Promise<void> {
    if (!this.deps) {
      return;
    }

    try {
      this.taskStartedCache.set(taskId, Date.now());
    } catch (err) {
      errorLog('LRUCache', `Failed to persist task start time for ${taskId}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  /**
   * Restore task state from LRU caches on startup.
   * Called in fetchCurrentTasks() for tasks with running=true.
   * Note: Only works within same process lifecycle.
   */
  public async restoreTaskState(taskId: string): Promise<boolean> {
    if (!this.deps) {
      return false;
    }

    try {
      // Restore taskStatus
      const status = this.taskStatusCache.get(taskId);
      if (status) {
        // Convert nextLoopTime strings back to Date objects
        if (status.marketplaces) {
          for (const mp of Object.keys(status.marketplaces)) {
            if (status.marketplaces[mp]?.nextLoopTime) {
              status.marketplaces[mp].nextLoopTime = new Date(status.marketplaces[mp].nextLoopTime);
            }
          }
        }
        this.deps.taskStatus[taskId] = status;
        debugLog('Recovery', `Restored taskStatus from LRU | taskId=${taskId}`);
      }

      // Restore loopCounterStats
      const loopCount = this.loopCountCache.get(taskId);
      if (loopCount) {
        this.deps.loopCounterStats[taskId] = loopCount;
        debugLog('Recovery', `Restored loopCounterStats from LRU | taskId=${taskId} | counts=${JSON.stringify(loopCount)}`);
      }

      // Restore lastScheduleTimes
      const lastSchedule = this.lastScheduleCache.get(taskId);
      if (lastSchedule) {
        this.deps.lastScheduleTimes.set(taskId, lastSchedule);
        debugLog('Recovery', `Restored lastScheduleTime from LRU | taskId=${taskId}`);
      }

      return !!status; // Return true if we found persisted state
    } catch (err) {
      errorLog('LRUCache', `Failed to restore task state for ${taskId}: ${err instanceof Error ? err.message : String(err)}`);
      return false;
    }
  }

  /**
   * Recreate timers from LRU cache after server restart.
   * Checks if timers should have fired during downtime and starts task immediately if so.
   * Note: Only works within same process lifecycle.
   */
  public async recreateTimersFromRedis(taskId: string, task: ITask): Promise<void> {
    if (!this.deps) {
      return;
    }

    try {
      const now = Date.now();

      for (const mp of task.selectedMarketplaces) {
        const mpLower = mp.toLowerCase();
        const nextLoopTime = this.nextLoopCache.get(`${taskId}:${mpLower}`);

        if (nextLoopTime) {
          const delay = Math.max(0, nextLoopTime - now);

          if (delay > 0) {
            // Timer hasn't fired yet - recreate it
            debugLog('Recovery', `Recreating timer for ${taskId}:${mpLower} | delay=${Math.floor(delay / 1000)}s`);

            // Set status to SCHEDULED with correct waitingTime
            const mpKey = mpLower as 'opensea' | 'magiceden' | 'blur';
            if (this.deps.taskStatus[taskId]?.marketplaces?.[mpKey]) {
              this.deps.taskStatus[taskId].marketplaces[mpKey]!.status = 'SCHEDULED';
              this.deps.taskStatus[taskId].marketplaces[mpKey]!.waitingTime = Math.floor(delay / 1000);
              this.deps.taskStatus[taskId].marketplaces[mpKey]!.nextLoopTime = new Date(nextLoopTime);
            }

            // Create the timer with remaining delay
            const timerKey = `${taskId}-${mpLower}`;
            const timeoutId = setTimeout(async () => {
              debugLog('Recovery', `Recovered timer EXPIRED | taskId=${taskId} | mp=${mpLower}`);
              const currentTask = this.deps!.activeTasks.get(taskId);
              if (currentTask?.running) {
                await this.deps!.startTask(currentTask, true, mpLower);
              }
            }, delay);

            this.deps.taskTimeouts.set(timerKey, timeoutId);

            // Also set up countdown timer
            const countdownTimer = setInterval(() => {
              const mpStatusRef = this.deps!.taskStatus[taskId]?.marketplaces?.[mpKey];
              if (!mpStatusRef) {
                clearInterval(countdownTimer);
                this.deps!.taskCountdownTimers.delete(timerKey);
                return;
              }
              if (mpStatusRef.nextLoopTime) {
                const remainingSeconds = Math.max(0, Math.floor((mpStatusRef.nextLoopTime.getTime() - Date.now()) / 1000));
                mpStatusRef.waitingTime = remainingSeconds;
                if (remainingSeconds <= 0) {
                  clearInterval(countdownTimer);
                  this.deps!.taskCountdownTimers.delete(timerKey);
                }
              }
            }, 1000);
            this.deps.taskCountdownTimers.set(timerKey, countdownTimer);

          } else {
            // Timer should have already fired during downtime - start task immediately
            debugLog('Recovery', `Timer expired during downtime for ${taskId}:${mpLower} | starting immediately`);
            await this.deps.startTask(task, true, mpLower);
          }

          // Clean up the cache key after processing
          this.nextLoopCache.delete(`${taskId}:${mpLower}`);
        }
      }
    } catch (err) {
      errorLog('LRUCache', `Failed to recreate timers for ${taskId}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  /**
   * Get service statistics
   */
  public getStats() {
    return {
      initialized: this.deps !== null,
      caches: {
        taskStatus: this.taskStatusCache.size,
        loopCount: this.loopCountCache.size,
        lastSchedule: this.lastScheduleCache.size,
        nextLoop: this.nextLoopCache.size,
        taskStarted: this.taskStartedCache.size,
      }
    };
  }
}

export const taskStatePersistenceService = new TaskStatePersistenceService();
