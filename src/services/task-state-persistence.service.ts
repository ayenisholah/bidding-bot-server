import { debugLog, errorLog } from '../utils/debug-file-logger';
import type { ITask } from '../models/task.model';

/**
 * Task State Persistence Service
 *
 * Manages task state persistence to Redis for crash recovery.
 *
 * Responsibilities:
 * - Persisting task state (status, loop counts, schedules) to Redis
 * - Restoring task state from Redis on server restart
 * - Recreating timers after server restart
 * - Task diagnostic logging for debugging stuck tasks
 *
 * Features:
 * - Automatic state persistence with TTL
 * - Timer recreation with downtime detection
 * - Comprehensive diagnostic snapshots
 * - Graceful error handling
 */

// Redis key prefixes
const REDIS_KEY_TASK_STATUS = 'task-status:';
const REDIS_KEY_LOOP_COUNT = 'loop-count:';
const REDIS_KEY_LAST_SCHEDULE = 'last-schedule:';
const REDIS_KEY_NEXT_LOOP = 'next-loop:';
const REDIS_KEY_TASK_STARTED = 'task-started:';
const REDIS_STATE_TTL = 86400; // 24 hours

/**
 * Dependencies required by TaskStatePersistenceService
 */
interface TaskStatePersistenceDependencies {
  getRedisConnection: () => any;
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

  /**
   * Initialize the service with dependencies
   */
  public initialize(deps: TaskStatePersistenceDependencies): void {
    this.deps = deps;
    debugLog('TaskStatePersistenceService', 'Initialized');
  }

  /**
   * Persist task state to Redis for crash recovery.
   * Called after setTaskStatus() to ensure state survives server restarts.
   */
  public async persistTaskState(taskId: string): Promise<void> {
    if (!this.deps) {
      errorLog('TaskStatePersistence', 'Cannot persist state - service not initialized');
      return;
    }

    try {
      const redis = this.deps.getRedisConnection();
      const status = this.deps.taskStatus[taskId];
      const loopCount = this.deps.loopCounterStats[taskId];
      const lastSchedule = this.deps.lastScheduleTimes.get(taskId);
      let _persisted = 0;

      // Persist taskStatus
      if (status) {
        await redis.set(
          `${REDIS_KEY_TASK_STATUS}${taskId}`,
          JSON.stringify(status),
          'EX', REDIS_STATE_TTL
        );
        _persisted++;
      }

      // Persist loopCounterStats
      if (loopCount) {
        await redis.set(
          `${REDIS_KEY_LOOP_COUNT}${taskId}`,
          JSON.stringify(loopCount),
          'EX', REDIS_STATE_TTL
        );
        _persisted++;
      }

      // Persist lastScheduleTimes
      if (lastSchedule) {
        await redis.set(
          `${REDIS_KEY_LAST_SCHEDULE}${taskId}`,
          lastSchedule.toString(),
          'EX', REDIS_STATE_TTL
        );
        _persisted++;
      }

      // Persist nextLoopTime for each marketplace
      if (status?.marketplaces) {
        for (const [mp, mpStatus] of Object.entries(status.marketplaces)) {
          if ((mpStatus as any)?.nextLoopTime) {
            await redis.set(
              `${REDIS_KEY_NEXT_LOOP}${taskId}:${mp}`,
              (mpStatus as any).nextLoopTime.getTime().toString(),
              'EX', REDIS_STATE_TTL
            );
            _persisted++;
          }
        }
      }
    } catch (err) {
      errorLog('RedisState', `PERSIST_FAILED | taskId=${taskId}`, err, { taskId });
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
   * Persist task start timestamp to Redis.
   * Called when a task first starts.
   */
  public async persistTaskStartTime(taskId: string): Promise<void> {
    if (!this.deps) {
      return;
    }

    try {
      const redis = this.deps.getRedisConnection();
      await redis.set(
        `${REDIS_KEY_TASK_STARTED}${taskId}`,
        Date.now().toString(),
        'EX', REDIS_STATE_TTL
      );
    } catch (err) {
      errorLog('Redis', `Failed to persist task start time for ${taskId}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  /**
   * Restore task state from Redis on startup.
   * Called in fetchCurrentTasks() for tasks with running=true.
   */
  public async restoreTaskState(taskId: string): Promise<boolean> {
    if (!this.deps) {
      return false;
    }

    try {
      const redis = this.deps.getRedisConnection();

      // Restore taskStatus
      const statusJson = await redis.get(`${REDIS_KEY_TASK_STATUS}${taskId}`);
      if (statusJson) {
        const status = JSON.parse(statusJson);
        // Convert nextLoopTime strings back to Date objects
        if (status.marketplaces) {
          for (const mp of Object.keys(status.marketplaces)) {
            if (status.marketplaces[mp]?.nextLoopTime) {
              status.marketplaces[mp].nextLoopTime = new Date(status.marketplaces[mp].nextLoopTime);
            }
          }
        }
        this.deps.taskStatus[taskId] = status;
        debugLog('Recovery', `Restored taskStatus from Redis | taskId=${taskId}`);
      }

      // Restore loopCounterStats
      const loopCountJson = await redis.get(`${REDIS_KEY_LOOP_COUNT}${taskId}`);
      if (loopCountJson) {
        this.deps.loopCounterStats[taskId] = JSON.parse(loopCountJson);
        debugLog('Recovery', `Restored loopCounterStats from Redis | taskId=${taskId} | counts=${JSON.stringify(this.deps.loopCounterStats[taskId])}`);
      }

      // Restore lastScheduleTimes
      const lastSchedule = await redis.get(`${REDIS_KEY_LAST_SCHEDULE}${taskId}`);
      if (lastSchedule) {
        this.deps.lastScheduleTimes.set(taskId, parseInt(lastSchedule, 10));
        debugLog('Recovery', `Restored lastScheduleTime from Redis | taskId=${taskId}`);
      }

      return !!statusJson; // Return true if we found persisted state
    } catch (err) {
      errorLog('Redis', `Failed to restore task state for ${taskId}: ${err instanceof Error ? err.message : String(err)}`);
      return false;
    }
  }

  /**
   * Recreate timers from Redis after server restart.
   * Checks if timers should have fired during downtime and starts task immediately if so.
   */
  public async recreateTimersFromRedis(taskId: string, task: ITask): Promise<void> {
    if (!this.deps) {
      return;
    }

    try {
      const redis = this.deps.getRedisConnection();
      const now = Date.now();

      for (const mp of task.selectedMarketplaces) {
        const mpLower = mp.toLowerCase();
        const nextLoopTimeStr = await redis.get(`${REDIS_KEY_NEXT_LOOP}${taskId}:${mpLower}`);

        if (nextLoopTimeStr) {
          const nextLoopTime = parseInt(nextLoopTimeStr, 10);
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

          // Clean up the Redis key after processing
          await redis.del(`${REDIS_KEY_NEXT_LOOP}${taskId}:${mpLower}`);
        }
      }
    } catch (err) {
      errorLog('Redis', `Failed to recreate timers for ${taskId}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  /**
   * Get service statistics
   */
  public getStats() {
    return {
      initialized: this.deps !== null
    };
  }
}

export const taskStatePersistenceService = new TaskStatePersistenceService();
