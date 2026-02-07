import { TaskBidData } from './task-metrics-types';
/**
 * BidRatesChangeTracker - Tracks changes to task bid data for delta updates
 *
 * This class:
 * - Tracks which tasks have changed (dirty flags)
 * - Stores the last known state of each task
 * - Computes delta updates (only changed fields)
 * - Provides efficient change detection using deep comparison
 */
class BidRatesChangeTracker {
  // Set of task IDs that have been modified since last flush
  private dirtyTasks: Set<string> = new Set();

  // Last known state of each task for delta computation
  private lastTaskStates: Map<string, TaskBidData> = new Map();

  // Last known global stats for delta detection
  private lastBidRates: { [key: string]: number } = {};
  private lastRps: { [key: string]: number } = {};

  /**
   * Mark a task as dirty (modified)
   */
  markDirty(taskId: string): void {
    this.dirtyTasks.add(taskId);
  }

  /**
   * Check if a task is dirty
   */
  isDirty(taskId: string): boolean {
    return this.dirtyTasks.has(taskId);
  }

  /**
   * Get all dirty task IDs
   */
  getDirtyTasks(): string[] {
    return Array.from(this.dirtyTasks);
  }

  /**
   * Clear dirty flag for a specific task
   */
  clearDirty(taskId: string): void {
    this.dirtyTasks.delete(taskId);
  }

  /**
   * Clear all dirty flags
   */
  clearAllDirty(): void {
    this.dirtyTasks.clear();
  }

  /**
   * Compute delta between last known state and new data
   * Returns null if nothing changed, otherwise returns only the changed fields
   */
  computeDelta(taskId: string, newData: TaskBidData): Partial<TaskBidData> | null {
    const lastState = this.lastTaskStates.get(taskId);

    // If no previous state, everything is new
    if (!lastState) {
      this.lastTaskStates.set(taskId, this.deepClone(newData));
      return newData;
    }

    const delta: Partial<TaskBidData> = {};
    let hasChanges = false;

    // Compare top-level numeric fields
    if (JSON.stringify(lastState.bidCounts) !== JSON.stringify(newData.bidCounts)) {
      delta.bidCounts = newData.bidCounts;
      hasChanges = true;
    }
    if (JSON.stringify(lastState.skipCounts) !== JSON.stringify(newData.skipCounts)) {
      delta.skipCounts = newData.skipCounts;
      hasChanges = true;
    }
    if (JSON.stringify(lastState.errorCounts) !== JSON.stringify(newData.errorCounts)) {
      delta.errorCounts = newData.errorCounts;
      hasChanges = true;
    }

    // Compare warning fields
    if (JSON.stringify(lastState.warningBids) !== JSON.stringify(newData.warningBids)) {
      delta.warningBids = newData.warningBids;
      hasChanges = true;
    }
    if (JSON.stringify(lastState.warningMessage) !== JSON.stringify(newData.warningMessage)) {
      delta.warningMessage = newData.warningMessage;
      hasChanges = true;
    }

    // Compare nested objects - floorPrices
    if (JSON.stringify(lastState.floorPrices) !== JSON.stringify(newData.floorPrices)) {
      delta.floorPrices = newData.floorPrices;
      hasChanges = true;
    }

    // Compare nested objects - bestOffers
    if (JSON.stringify(lastState.bestOffers) !== JSON.stringify(newData.bestOffers)) {
      delta.bestOffers = newData.bestOffers;
      hasChanges = true;
    }

    // Compare nested objects - ownBidCounts
    if (JSON.stringify(lastState.ownBidCounts) !== JSON.stringify(newData.ownBidCounts)) {
      delta.ownBidCounts = newData.ownBidCounts;
      hasChanges = true;
    }

    // Compare nested objects - loopCounterStats
    if (JSON.stringify(lastState.loopCounterStats) !== JSON.stringify(newData.loopCounterStats)) {
      delta.loopCounterStats = newData.loopCounterStats;
      hasChanges = true;
    }

    // Compare nested objects - counterbidCounts
    if (JSON.stringify(lastState.counterbidCounts) !== JSON.stringify(newData.counterbidCounts)) {
      delta.counterbidCounts = newData.counterbidCounts;
      hasChanges = true;
    }

    // Compare taskStatus (more complex nested structure)
    const taskStatusDelta = this.compareTaskStatus(lastState.taskStatus, newData.taskStatus);
    if (taskStatusDelta) {
      delta.taskStatus = taskStatusDelta;
      hasChanges = true;
    }

    if (hasChanges) {
      // Update last known state
      this.lastTaskStates.set(taskId, this.deepClone(newData));
      return delta;
    }

    return null;
  }

  /**
   * Compare marketplace number objects (opensea, blur, magiceden)
   * Returns the full object if any field changed, null otherwise
   */
  private compareMarketplaceNumbers(
    last: { opensea: number; blur: number; magiceden: number } | undefined,
    current: { opensea: number; blur: number; magiceden: number }
  ): { opensea: number; blur: number; magiceden: number } | null {
    if (!last) return current;

    if (
      last.opensea !== current.opensea ||
      last.blur !== current.blur ||
      last.magiceden !== current.magiceden
    ) {
      return current;
    }
    return null;
  }

  /**
   * Compare taskStatus objects
   * Returns the full object if any field changed, null otherwise
   */
  private compareTaskStatus(
    last: TaskBidData['taskStatus'] | undefined,
    current: TaskBidData['taskStatus']
  ): TaskBidData['taskStatus'] | null {
    if (!last) return current;

    // Compare top-level fields
    if (last.running !== current.running ||
        last.runningPercentage !== current.runningPercentage ||
        last.cancellationPercentage !== current.cancellationPercentage) {
      return current;
    }

    // Compare marketplaceStatus
    const marketplaces = ['opensea', 'magiceden', 'blur'] as const;
    for (const mp of marketplaces) {
      const lastMp = last.marketplaceStatus?.[mp];
      const currentMp = current.marketplaceStatus?.[mp];

      if (!lastMp && currentMp) {
        return current;
      }
      if (lastMp && !currentMp) {
        return current;
      }
      if (lastMp && currentMp) {
        if (lastMp.status !== currentMp.status || lastMp.statusReason !== currentMp.statusReason) {
          return current;
        }
      }
    }

    return null;
  }

  /**
   * Check if global stats (bidRates, rps) have changed
   * Returns true if there are changes
   */
  hasGlobalChanges(bidRates: { [key: string]: number }, rps: { [key: string]: number }): boolean {
    // Compare bidRates
    if (!this.compareObjects(this.lastBidRates, bidRates)) {
      return true;
    }

    // Compare rps
    if (!this.compareObjects(this.lastRps, rps)) {
      return true;
    }

    return false;
  }

  /**
   * Update last known global stats
   */
  updateLastGlobalStats(bidRates: { [key: string]: number }, rps: { [key: string]: number }): void {
    this.lastBidRates = { ...bidRates };
    this.lastRps = { ...rps };
  }

  /**
   * Compare two objects for equality
   */
  private compareObjects(a: { [key: string]: number }, b: { [key: string]: number }): boolean {
    const keysA = Object.keys(a);
    const keysB = Object.keys(b);

    if (keysA.length !== keysB.length) return false;

    for (const key of keysA) {
      if (a[key] !== b[key]) return false;
    }

    return true;
  }

  /**
   * Remove task state (call when task is stopped/deleted)
   */
  removeTask(taskId: string): void {
    this.dirtyTasks.delete(taskId);
    this.lastTaskStates.delete(taskId);
  }

  /**
   * Clear all stored states
   */
  clear(): void {
    this.dirtyTasks.clear();
    this.lastTaskStates.clear();
    this.lastBidRates = {};
    this.lastRps = {};
  }

  /**
   * Get stats about the tracker
   */
  getStats(): { dirtyCount: number; trackedTasks: number } {
    return {
      dirtyCount: this.dirtyTasks.size,
      trackedTasks: this.lastTaskStates.size
    };
  }

  /**
   * Deep clone an object to avoid reference issues
   */
  private deepClone<T>(obj: T): T {
    return JSON.parse(JSON.stringify(obj));
  }
}

// Singleton instance
export const bidRatesChangeTracker = new BidRatesChangeTracker();
