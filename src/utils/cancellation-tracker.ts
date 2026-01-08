/**
 * Cancellation Tracker
 *
 * Tracks bid cancellation progress for tasks.
 * - Fetches total bid count from MongoDB
 * - Tracks individual cancellation completions
 * - Calculates percentage as completed/totalBids
 * - Thread-safe for concurrent operations
 */

import { debugLog, infoLog } from './debug-file-logger';

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
    infoLog('CancellationTracking', `INIT_TRACKING | taskId=${taskId} | totalBids=${totalBids} | marketplace=${marketplace || 'global'} | key=${key}`);

    this.trackingMap.set(key, {
      taskId,
      marketplace,
      totalBids,
      completedCancellations: 0,
      startTime: Date.now()
    });

    infoLog('CancellationTracking', `INIT_TRACKING_DONE | key=${key} | mapSize=${this.trackingMap.size} | state={completed:0, total:${totalBids}}`);
  }

  /**
   * Set marketplace-specific totals for global cancellations
   */
  setMarketplaceTotals(taskId: string, marketplaceTotals: { opensea: number; magiceden: number; blur: number }): void {
    infoLog('CancellationTracking', `SET_MP_TOTALS | taskId=${taskId} | opensea=${marketplaceTotals.opensea} | magiceden=${marketplaceTotals.magiceden} | blur=${marketplaceTotals.blur}`);

    Object.entries(marketplaceTotals).forEach(([marketplace, total]) => {
      const cacheKey = `${taskId}-${marketplace}-total`;
      this.marketplaceTotalsCache.set(cacheKey, total);
      debugLog('CancellationTracking', `SET_MP_TOTAL_ENTRY | cacheKey=${cacheKey} | total=${total}`);
    });

    infoLog('CancellationTracking', `SET_MP_TOTALS_DONE | taskId=${taskId} | cacheSize=${this.marketplaceTotalsCache.size}`);
  }

  /**
   * Record successful cancellation(s)
   * @param completionCount Number of cancellations completed (default: 1)
   */
  recordCompletion(taskId: string, completionCount: number = 1, marketplace?: string): void {
    debugLog('CancellationTracking', `RECORD_COMPLETION | taskId=${taskId} | count=${completionCount} | marketplace=${marketplace || 'global'}`);

    // For global cancellations, we need to initialize marketplace-specific tracking
    if (this.hasTaskLevelTracking(taskId) && marketplace) {
      // Initialize marketplace tracking if it doesn't exist
      const marketplaceKey = this.getKey(taskId, marketplace);
      if (!this.trackingMap.has(marketplaceKey)) {
        // Get marketplace-specific totals from parent cache if available
        const parentCacheKey = `${taskId}-${marketplace}-total`;
        const marketplaceTotalBids = this.marketplaceTotalsCache.get(parentCacheKey) || 0;

        infoLog('CancellationTracking', `RECORD_AUTO_INIT | taskId=${taskId} | marketplace=${marketplace} | marketplaceKey=${marketplaceKey} | marketplaceTotalBids=${marketplaceTotalBids} | parentCacheKey=${parentCacheKey}`);

        this.trackingMap.set(marketplaceKey, {
          taskId,
          marketplace,
          totalBids: marketplaceTotalBids,
          completedCancellations: 0,
          startTime: Date.now()
        });

        infoLog('CancellationTracking', `RECORD_AUTO_INIT_DONE | marketplaceKey=${marketplaceKey} | mapSize=${this.trackingMap.size}`);
      }
    }

    const key = this.getKey(taskId, marketplace);
    const tracking = this.trackingMap.get(key);

    if (tracking) {
      const before = tracking.completedCancellations;
      // Cap completions at total bids to prevent overflow
      tracking.completedCancellations = Math.min(tracking.completedCancellations + completionCount, tracking.totalBids);
      const after = tracking.completedCancellations;

      debugLog('CancellationTracking', `RECORD_COMPLETION_DONE | key=${key} | before=${before} | count=${completionCount} | after=${after} | total=${tracking.totalBids} | pct=${((after / tracking.totalBids) * 100).toFixed(2)}%`);
    } else {
      infoLog('CancellationTracking', `RECORD_NO_TRACKING | taskId=${taskId} | marketplace=${marketplace || 'global'} | key=${key} | trackingNotFound=true`);
    }
  }

  /**
   * Get current cancellation percentage
   * @returns Percentage (0-100) or null if not tracking
   */
  getPercentage(taskId: string, marketplace?: string): number | null {
    const key = this.getKey(taskId, marketplace);
    const tracking = this.trackingMap.get(key);

    debugLog('CancellationTracking', `GET_PCT | taskId=${taskId} | marketplace=${marketplace || 'global'} | key=${key} | hasTracking=${!!tracking}`);

    if (!tracking) {
      debugLog('CancellationTracking', `GET_PCT_NO_TRACKING | key=${key} | result=null`);
      return null;
    }

    if (tracking.totalBids === 0) {
      debugLog('CancellationTracking', `GET_PCT_ZERO_BIDS | key=${key} | result=100`);
      return 100; // No bids to cancel = 100% complete
    }

    // Calculate percentage with 2 decimal places
    const percentage = (tracking.completedCancellations / tracking.totalBids) * 100;
    const result = Math.min(100, parseFloat(percentage.toFixed(2)));

    debugLog('CancellationTracking', `GET_PCT_RESULT | key=${key} | completed=${tracking.completedCancellations} | total=${tracking.totalBids} | result=${result}%`);

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

    debugLog('CancellationTracking', `GET_AGG_PCT | taskId=${taskId} | taskLevelKey=${taskLevelKey} | hasTaskLevelTracking=${!!taskLevelTracking}`);

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

      debugLog('CancellationTracking', `GET_AGG_PCT_BREAKDOWN | taskId=${taskId} | opensea=${mpBreakdown.opensea} | magiceden=${mpBreakdown.magiceden} | blur=${mpBreakdown.blur} | totalCompleted=${totalCompleted} | taskLevelTotal=${taskLevelTracking.totalBids}`);

      // Calculate percentage based on task-level total
      if (taskLevelTracking.totalBids === 0) {
        debugLog('CancellationTracking', `GET_AGG_PCT_ZERO_BIDS | taskId=${taskId} | result=100`);
        return 100;
      }

      const percentage = (totalCompleted / taskLevelTracking.totalBids) * 100;
      const result = Math.min(100, parseFloat(percentage.toFixed(2)));

      infoLog('CancellationTracking', `GET_AGG_PCT_RESULT | taskId=${taskId} | completed=${totalCompleted} | total=${taskLevelTracking.totalBids} | result=${result}%`);

      return result;
    }

    // No task-level tracking, return null
    debugLog('CancellationTracking', `GET_AGG_PCT_NO_TRACKING | taskId=${taskId} | result=null`);
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

    infoLog('CancellationTracking', `CLEAR_TRACKING | taskId=${taskId} | marketplace=${marketplace || 'global'} | key=${key} | hadEntry=${hadEntry}`);

    this.trackingMap.delete(key);

    // Clear marketplace totals cache if clearing task-level tracking
    if (!marketplace) {
      const marketplaces = ['opensea', 'magiceden', 'blur'];
      const deletedCacheKeys: string[] = [];
      marketplaces.forEach(mp => {
        const cacheKey = `${taskId}-${mp}-total`;
        if (this.marketplaceTotalsCache.has(cacheKey)) {
          deletedCacheKeys.push(cacheKey);
        }
        this.marketplaceTotalsCache.delete(cacheKey);
      });

      infoLog('CancellationTracking', `CLEAR_TRACKING_CACHES | taskId=${taskId} | deletedCacheKeys=${deletedCacheKeys.join(',')} | remainingMapSize=${this.trackingMap.size} | remainingCacheSize=${this.marketplaceTotalsCache.size}`);
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
