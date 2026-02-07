// Constants
const LOCK_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes
const SCHEDULED_BID_LOCK_DURATION_MS = 15 * 60 * 1000; // 15 minutes
const LOCK_EXTENSION_MS = 5 * 60 * 1000; // 5 minutes
const LOCK_EXTENSION_THRESHOLD_MS = 2 * 60 * 1000; // 2 minutes

// Type definitions
interface TaskLock {
  locked: boolean;
  lockUntil: number;
  lastReleaseTime?: number;
}

interface LoopSchedulingLock {
  locked: boolean;
  timestamp: number;
}

/**
 * Lock Management Service
 *
 * Provides centralized lock management for task operations to prevent race conditions.
 *
 * Lock Types:
 * - Task Locks: Simple boolean locks for task-level operations
 * - Loop Scheduling Locks: Prevents multiple bidding loops from running simultaneously
 * - Scheduled Bid Locks: Prevents concurrent execution of scheduled bid processing
 *
 * Features:
 * - Automatic stale lock detection and cleanup
 * - Lock extension for long-running operations
 * - Thread-safe lock acquisition and release
 */
class LockManagementService {
  // Task-level locks
  private taskLocks = new Map<string, TaskLock>();

  // Loop scheduling locks (prevents multiple bidding loops)
  private loopSchedulingLocks = new Map<string, LoopSchedulingLock>();

  /**
   * Initialize the service
   */
  public initialize(): void {
  }

  // =============================================================================
  // TASK LOCKS
  // =============================================================================

  /**
   * Unlock a task (simple boolean unlock)
   */
  public unlockTask(taskId: string): void {
    const currentLock = this.taskLocks.get(taskId);
    if (currentLock) {
      currentLock.locked = false;
    }
  }

  // =============================================================================
  // LOOP SCHEDULING LOCKS
  // =============================================================================

  /**
   * Acquire loop scheduling lock to prevent multiple bidding loops from running
   * Returns true if lock acquired, false if already locked
   */
  public acquireLoopSchedulingLock(taskId: string): boolean {
    const existingLock = this.loopSchedulingLocks.get(taskId);
    const now = Date.now();

    // Check if lock exists and is not stale
    if (existingLock && existingLock.locked) {
      // Check if lock is stale (older than 5 minutes)
      if (now - existingLock.timestamp > LOCK_TIMEOUT_MS) {
        this.loopSchedulingLocks.delete(taskId);
      } else {
        return false;
      }
    }

    this.loopSchedulingLocks.set(taskId, { locked: true, timestamp: now });
    return true;
  }

  /**
   * Release loop scheduling lock
   */
  public releaseLoopSchedulingLock(taskId: string): void {
    if (this.loopSchedulingLocks.has(taskId)) {
      this.loopSchedulingLocks.delete(taskId);
    }
  }

  // =============================================================================
  // SCHEDULED BID LOCKS
  // =============================================================================

  /**
   * Acquire scheduled bid lock with stale detection
   * Throws error if lock already held or stale
   */
  public acquireScheduledBidLock(taskId: string, marketplace: string): void {
    const lockKey = `${taskId}-${marketplace}-scheduled`;
    const existingLock = this.taskLocks.get(lockKey);
    const now = Date.now();

    if (existingLock && existingLock.locked) {
      // Check if lock is stale (expired)
      if (existingLock.lockUntil && now > existingLock.lockUntil) {
        // Throw error for stale lock - let retry policy handle it
        const staleTime = now - existingLock.lockUntil;
        throw new Error(`Stale lock detected for task ${taskId} in ${marketplace} scheduled bid. Lock expired ${staleTime}ms ago`);
      }
      // Lock is held by another process - throw error to skip
      throw new Error(`Task ${taskId} is already being processed by ${marketplace} scheduled bid function`);
    }

    // Acquire lock for 15 minutes (matching job timeout)
    this.taskLocks.set(lockKey, {
      locked: true,
      lockUntil: now + SCHEDULED_BID_LOCK_DURATION_MS,
      lastReleaseTime: undefined
    });
  }

  /**
   * Extend scheduled bid lock during long operations
   * Returns true if extended, false if lock not held or doesn't need extension
   */
  public extendScheduledBidLock(
    taskId: string,
    marketplace: string,
    extensionMs: number = LOCK_EXTENSION_MS
  ): boolean {
    const lockKey = `${taskId}-${marketplace}-scheduled`;
    const lock = this.taskLocks.get(lockKey);

    if (!lock || !lock.locked) {
      return false;
    }

    const now = Date.now();

    // Only extend if lock is about to expire (within 2 minutes)
    if (lock.lockUntil && (lock.lockUntil - now) < LOCK_EXTENSION_THRESHOLD_MS) {
      lock.lockUntil = now + extensionMs;
      return true;
    }

    return false;
  }

  /**
   * Release scheduled bid lock
   */
  public releaseScheduledBidLock(taskId: string, marketplace: string): void {
    const lockKey = `${taskId}-${marketplace}-scheduled`;
    const lock = this.taskLocks.get(lockKey);
    if (lock) {
      lock.lastReleaseTime = Date.now();
      this.taskLocks.delete(lockKey);
    }
  }

  // =============================================================================
  // DIAGNOSTICS & CLEANUP
  // =============================================================================

  /**
   * Check if a task has a loop scheduling lock
   */
  public hasLoopSchedulingLock(taskId: string): boolean {
    return this.loopSchedulingLocks.has(taskId);
  }

  /**
   * Get all loop scheduling locks (for diagnostics)
   */
  public getAllLoopSchedulingLocks(): Array<{ taskId: string; locked: boolean; timestamp: number }> {
    const locks: Array<{ taskId: string; locked: boolean; timestamp: number }> = [];
    this.loopSchedulingLocks.forEach((lock, taskId) => {
      locks.push({ taskId, ...lock });
    });
    return locks;
  }

  /**
   * Get statistics about current locks
   */
  public getStats(): {
    taskLocks: number;
    loopSchedulingLocks: number;
    staleLocks: number;
  } {
    const now = Date.now();
    let staleCount = 0;

    // Count stale task locks
    for (const [_key, lock] of this.taskLocks.entries()) {
      if (lock.locked && lock.lockUntil && now > lock.lockUntil) {
        staleCount++;
      }
    }

    // Count stale loop locks
    for (const [_key, lock] of this.loopSchedulingLocks.entries()) {
      if (lock.locked && (now - lock.timestamp) > LOCK_TIMEOUT_MS) {
        staleCount++;
      }
    }

    return {
      taskLocks: this.taskLocks.size,
      loopSchedulingLocks: this.loopSchedulingLocks.size,
      staleLocks: staleCount
    };
  }

  /**
   * Clean up stale locks
   * Returns number of locks cleaned
   */
  public cleanupStaleLocks(): number {
    const now = Date.now();
    let cleanedCount = 0;

    // Clean stale task locks
    for (const [key, lock] of this.taskLocks.entries()) {
      if (lock.locked && lock.lockUntil && now > lock.lockUntil) {
        this.taskLocks.delete(key);
        cleanedCount++;
      }
    }

    // Clean stale loop scheduling locks
    for (const [key, lock] of this.loopSchedulingLocks.entries()) {
      if (lock.locked && (now - lock.timestamp) > LOCK_TIMEOUT_MS) {
        this.loopSchedulingLocks.delete(key);
        cleanedCount++;
      }
    }

    if (cleanedCount > 0) {
    }

    return cleanedCount;
  }

  /**
   * Clear all locks for a specific task (used during task cleanup)
   */
  public clearTaskLocks(taskId: string): void {
    // Clear task lock
    this.taskLocks.delete(taskId);

    // Clear loop scheduling lock
    this.loopSchedulingLocks.delete(taskId);

    // Clear scheduled bid locks for all marketplaces
    const marketplaces = ['opensea', 'magiceden', 'blur'];
    for (const marketplace of marketplaces) {
      const lockKey = `${taskId}-${marketplace}-scheduled`;
      this.taskLocks.delete(lockKey);
    }
  }
}

export const lockManagementService = new LockManagementService();
