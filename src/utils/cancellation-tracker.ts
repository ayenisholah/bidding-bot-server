/**
 * Cancellation Tracker
 *
 * Tracks bid cancellation progress for tasks.
 * - Fetches total bid count from MongoDB
 * - Tracks individual cancellation completions
 * - Calculates percentage as completed/totalBids
 * - Thread-safe for concurrent operations
 */

import { debugLog, infoLog, warnLog } from './debug-file-logger';

// Note: Using 'StopTracker' prefix to route logs to stoptask-YYYY-MM-DD.log
// for easier correlation with stopTask debug logging

interface CancellationTracking {
  taskId: string;
  marketplace?: string;
  totalBids: number;
  completedCancellations: number;
  startTime: number;
}

class CancellationTracker {
  private trackingMap: Map<string, CancellationTracking> = new Map();
  private marketplaceTotalsCache: Map<string, number> = new Map();

  /**
   * Initialize tracking for a task cancellation
   */
  initializeTracking(taskId: string, totalBids: number, marketplace?: string): void {
    const key = this.getKey(taskId, marketplace);
    infoLog('StopTracker', `INIT_TRACKING | taskId=${taskId} | totalBids=${totalBids} | marketplace=${marketplace || 'global'} | key=${key}`);

    this.trackingMap.set(key, {
      taskId,
      marketplace,
      totalBids,
      completedCancellations: 0,
      startTime: Date.now()
    });

    infoLog('StopTracker', `INIT_TRACKING_DONE | key=${key} | mapSize=${this.trackingMap.size} | state={completed:0, total:${totalBids}}`);
  }

  /**
   * Set marketplace-specific totals for global cancellations
   */
  setMarketplaceTotals(taskId: string, marketplaceTotals: { opensea: number; magiceden: number; blur: number }): void {
    infoLog('StopTracker', `SET_MP_TOTALS | taskId=${taskId} | opensea=${marketplaceTotals.opensea} | magiceden=${marketplaceTotals.magiceden} | blur=${marketplaceTotals.blur}`);

    Object.entries(marketplaceTotals).forEach(([marketplace, total]) => {
      const cacheKey = `${taskId}-${marketplace}-total`;
      this.marketplaceTotalsCache.set(cacheKey, total);
      debugLog('StopTracker', `SET_MP_TOTAL_ENTRY | cacheKey=${cacheKey} | total=${total}`);
    });

    infoLog('StopTracker', `SET_MP_TOTALS_DONE | taskId=${taskId} | cacheSize=${this.marketplaceTotalsCache.size}`);
  }

  /**
   * Record successful cancellation(s)
   * @param completionCount Number of cancellations completed (default: 1)
   */
  recordCompletion(taskId: string, completionCount: number = 1, marketplace?: string): void {
    debugLog('StopTracker', `RECORD_COMPLETION | taskId=${taskId} | count=${completionCount} | marketplace=${marketplace || 'global'}`);

    // For global cancellations, we need to initialize marketplace-specific tracking
    if (this.hasTaskLevelTracking(taskId) && marketplace) {
      // Initialize marketplace tracking if it doesn't exist
      const marketplaceKey = this.getKey(taskId, marketplace);
      if (!this.trackingMap.has(marketplaceKey)) {
        // Get marketplace-specific totals from parent cache if available
        const parentCacheKey = `${taskId}-${marketplace}-total`;
        const marketplaceTotalBids = this.marketplaceTotalsCache.get(parentCacheKey) || 0;

        infoLog('StopTracker', `RECORD_AUTO_INIT | taskId=${taskId} | marketplace=${marketplace} | marketplaceKey=${marketplaceKey} | marketplaceTotalBids=${marketplaceTotalBids} | parentCacheKey=${parentCacheKey}`);

        this.trackingMap.set(marketplaceKey, {
          taskId,
          marketplace,
          totalBids: marketplaceTotalBids,
          completedCancellations: 0,
          startTime: Date.now()
        });

        infoLog('StopTracker', `RECORD_AUTO_INIT_DONE | marketplaceKey=${marketplaceKey} | mapSize=${this.trackingMap.size}`);
      }
    }

    // Update marketplace-level tracking (if marketplace specified)
    const key = this.getKey(taskId, marketplace);
    const tracking = this.trackingMap.get(key);

    if (tracking) {
      const before = tracking.completedCancellations;
      // Cap completions at total bids to prevent overflow
      tracking.completedCancellations = Math.min(tracking.completedCancellations + completionCount, tracking.totalBids);
      const after = tracking.completedCancellations;

      debugLog('StopTracker', `RECORD_COMPLETION_DONE | key=${key} | before=${before} | count=${completionCount} | after=${after} | total=${tracking.totalBids} | pct=${((after / tracking.totalBids) * 100).toFixed(2)}%`);
    } else {
      // Late completion arrival - tracking was already cleared (task restarted or cancellation completed)
      // This is expected behavior, not a warning - silently ignore to reduce log noise
      // The 8,926 RECORD_NO_TRACKING warnings in logs are from this benign race condition
      debugLog('StopTracker', `RECORD_NO_TRACKING | taskId=${taskId} | marketplace=${marketplace || 'global'} | key=${key} | reason=tracking_already_cleared_expected_late_arrival`);
    }

    // Fix 4: ALSO update task-level tracking when marketplace-specific completion is recorded
    // This ensures the dashboard shows non-zero completion at task level
    if (marketplace) {
      const taskKey = this.getKey(taskId); // No marketplace = task-level key
      const taskTracking = this.trackingMap.get(taskKey);
      if (taskTracking) {
        const taskBefore = taskTracking.completedCancellations;
        taskTracking.completedCancellations = Math.min(taskTracking.completedCancellations + completionCount, taskTracking.totalBids);
        const taskAfter = taskTracking.completedCancellations;
        debugLog('StopTracker', `RECORD_COMPLETION_TASK_LEVEL | taskId=${taskId} | before=${taskBefore} | count=${completionCount} | after=${taskAfter} | total=${taskTracking.totalBids}`);
      }
    }
  }

  /**
   * Get current cancellation percentage
   * @returns Percentage (0-100) or null if not tracking
   */
  getPercentage(taskId: string, marketplace?: string): number | null {
    const key = this.getKey(taskId, marketplace);
    const tracking = this.trackingMap.get(key);

    debugLog('StopTracker', `GET_PCT | taskId=${taskId} | marketplace=${marketplace || 'global'} | key=${key} | hasTracking=${!!tracking}`);

    if (!tracking) {
      debugLog('StopTracker', `GET_PCT_NO_TRACKING | key=${key} | result=null`);
      return null;
    }

    if (tracking.totalBids === 0) {
      debugLog('StopTracker', `GET_PCT_ZERO_BIDS | key=${key} | result=100`);
      return 100; // No bids to cancel = 100% complete
    }

    // Calculate percentage with 2 decimal places
    const percentage = (tracking.completedCancellations / tracking.totalBids) * 100;
    const result = Math.min(100, parseFloat(percentage.toFixed(2)));

    debugLog('StopTracker', `GET_PCT_RESULT | key=${key} | completed=${tracking.completedCancellations} | total=${tracking.totalBids} | result=${result}%`);

    return result;
  }

  /**
   * Get tracking status for a task
   */
  getStatus(taskId: string, marketplace?: string): CancellationTracking | null {
    const key = this.getKey(taskId, marketplace);
    return this.trackingMap.get(key) || null;
  }

  /**
   * Check if cancellation is complete
   */
  isComplete(taskId: string, marketplace?: string): boolean {
    const key = this.getKey(taskId, marketplace);
    const tracking = this.trackingMap.get(key);

    if (!tracking) {
      return false;
    }

    return tracking.completedCancellations >= tracking.totalBids;
  }

  /**
   * Get aggregated cancellation percentage across all marketplaces
   * Used when global cancellation is tracked at task level
   */
  getAggregatedPercentage(taskId: string): number | null {
    // Check if we have task-level tracking (no marketplace)
    const taskLevelKey = this.getKey(taskId);
    const taskLevelTracking = this.trackingMap.get(taskLevelKey);

    debugLog('StopTracker', `GET_AGG_PCT | taskId=${taskId} | taskLevelKey=${taskLevelKey} | hasTaskLevelTracking=${!!taskLevelTracking}`);

    if (taskLevelTracking) {
      // We have task-level tracking, calculate from marketplace completions
      let totalCompleted = 0;
      const marketplaces = ['opensea', 'magiceden', 'blur'];
      const mpBreakdown: Record<string, number> = {};

      // Sum up completions from each marketplace
      marketplaces.forEach(marketplace => {
        const marketplaceKey = this.getKey(taskId, marketplace);
        const marketplaceTracking = this.trackingMap.get(marketplaceKey);
        if (marketplaceTracking) {
          totalCompleted += marketplaceTracking.completedCancellations;
          mpBreakdown[marketplace] = marketplaceTracking.completedCancellations;
        } else {
          mpBreakdown[marketplace] = 0;
        }
      });

      debugLog('StopTracker', `GET_AGG_PCT_BREAKDOWN | taskId=${taskId} | opensea=${mpBreakdown.opensea} | magiceden=${mpBreakdown.magiceden} | blur=${mpBreakdown.blur} | totalCompleted=${totalCompleted} | taskLevelTotal=${taskLevelTracking.totalBids}`);

      // Calculate percentage based on task-level total
      if (taskLevelTracking.totalBids === 0) {
        debugLog('StopTracker', `GET_AGG_PCT_ZERO_BIDS | taskId=${taskId} | result=100`);
        return 100;
      }

      const percentage = (totalCompleted / taskLevelTracking.totalBids) * 100;
      const result = Math.min(100, parseFloat(percentage.toFixed(2)));

      infoLog('StopTracker', `GET_AGG_PCT_RESULT | taskId=${taskId} | completed=${totalCompleted} | total=${taskLevelTracking.totalBids} | result=${result}%`);

      return result;
    }

    // No task-level tracking, return null
    debugLog('StopTracker', `GET_AGG_PCT_NO_TRACKING | taskId=${taskId} | result=null`);
    return null;
  }

  /**
   * Check if task-level tracking exists (without marketplace)
   */
  hasTaskLevelTracking(taskId: string): boolean {
    const key = this.getKey(taskId); // No marketplace parameter
    return this.trackingMap.has(key);
  }

  /**
   * Clear tracking for a task
   */
  clearTracking(taskId: string, marketplace?: string): void {
    const key = this.getKey(taskId, marketplace);
    const hadEntry = this.trackingMap.has(key);

    infoLog('StopTracker', `CLEAR_TRACKING | taskId=${taskId} | marketplace=${marketplace || 'global'} | key=${key} | hadEntry=${hadEntry}`);

    this.trackingMap.delete(key);

    // Clear marketplace totals cache AND marketplace-specific tracking entries if clearing task-level tracking
    if (!marketplace) {
      const marketplaces = ['opensea', 'magiceden', 'blur'];
      const deletedCacheKeys: string[] = [];
      const deletedTrackingKeys: string[] = [];

      marketplaces.forEach(mp => {
        // Clear cache entries
        const cacheKey = `${taskId}-${mp}-total`;
        if (this.marketplaceTotalsCache.has(cacheKey)) {
          deletedCacheKeys.push(cacheKey);
        }
        this.marketplaceTotalsCache.delete(cacheKey);

        // Fix 13: Also clear marketplace-specific tracking entries to prevent stale entries
        // from previous cancellation attempts causing premature 100% completion detection
        const trackingKey = this.getKey(taskId, mp);
        if (this.trackingMap.has(trackingKey)) {
          deletedTrackingKeys.push(trackingKey);
        }
        this.trackingMap.delete(trackingKey);
      });

      infoLog('StopTracker', `CLEAR_TRACKING_CACHES | taskId=${taskId} | deletedCacheKeys=${deletedCacheKeys.join(',') || 'none'} | deletedTrackingKeys=${deletedTrackingKeys.join(',') || 'none'} | remainingMapSize=${this.trackingMap.size} | remainingCacheSize=${this.marketplaceTotalsCache.size}`);
    }
  }

  /**
   * Get all active trackings
   */
  getAllTrackings(): Map<string, CancellationTracking> {
    return new Map(this.trackingMap);
  }

  /**
   * Generate consistent key for tracking
   */
  private getKey(taskId: string, marketplace?: string): string {
    return marketplace ? `${taskId}-${marketplace.toLowerCase()}` : taskId;
  }
}

// Export singleton instance
export const cancellationTracker = new CancellationTracker();
