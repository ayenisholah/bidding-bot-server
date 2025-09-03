/**
 * Cancellation Tracker
 * 
 * Tracks bid cancellation progress for tasks.
 * - Fetches total bid count from MongoDB
 * - Tracks individual cancellation completions
 * - Calculates percentage as completed/totalBids
 * - Thread-safe for concurrent operations
 */

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
    console.log(`[CancellationTracker] Initializing tracking - Key: ${key}, TotalBids: ${totalBids}`);
    this.trackingMap.set(key, {
      taskId,
      marketplace,
      totalBids,
      completedCancellations: 0,
      startTime: Date.now()
    });
    console.log(`[CancellationTracker] Tracking initialized successfully for ${key}`);
  }

  /**
   * Set marketplace-specific totals for global cancellations
   */
  setMarketplaceTotals(taskId: string, marketplaceTotals: { opensea: number; magiceden: number; blur: number }): void {
    Object.entries(marketplaceTotals).forEach(([marketplace, total]) => {
      const cacheKey = `${taskId}-${marketplace}-total`;
      this.marketplaceTotalsCache.set(cacheKey, total);
      // console.log(`[CancellationTracker] Set marketplace total - ${marketplace}: ${total}`);
    });
  }

  /**
   * Record successful cancellation(s)
   * @param completionCount Number of cancellations completed (default: 1)
   */
  recordCompletion(taskId: string, completionCount: number = 1, marketplace?: string): void {
    // For global cancellations, we need to initialize marketplace-specific tracking
    if (this.hasTaskLevelTracking(taskId) && marketplace) {
      // Initialize marketplace tracking if it doesn't exist
      const marketplaceKey = this.getKey(taskId, marketplace);
      if (!this.trackingMap.has(marketplaceKey)) {
        // Get marketplace-specific totals from parent cache if available
        const parentCacheKey = `${taskId}-${marketplace}-total`;
        const marketplaceTotalBids = this.marketplaceTotalsCache.get(parentCacheKey) || 0;
        
        this.trackingMap.set(marketplaceKey, {
          taskId,
          marketplace,
          totalBids: marketplaceTotalBids,
          completedCancellations: 0,
          startTime: Date.now()
        });
        
        // console.log(`[CancellationTracker] Initialized marketplace tracking - Key: ${marketplaceKey}, TotalBids: ${marketplaceTotalBids}`);
      }
    }
    
    const key = this.getKey(taskId, marketplace);
    const tracking = this.trackingMap.get(key);
    
    if (tracking) {
      // Cap completions at total bids to prevent overflow
      const previousCompleted = tracking.completedCancellations;
      tracking.completedCancellations = Math.min(tracking.completedCancellations + completionCount, tracking.totalBids);
      const actualIncrement = tracking.completedCancellations - previousCompleted;
      
      const percentage = tracking.totalBids > 0 ? ((tracking.completedCancellations / tracking.totalBids) * 100).toFixed(2) : '0.00';
      
      if (actualIncrement < completionCount) {
        console.log(`[CancellationTracker] Capped completion - Key: ${key}, Requested: ${completionCount}, Actual: ${actualIncrement}, Total: ${tracking.completedCancellations}/${tracking.totalBids} (${percentage}%)`);
      } else {
        console.log(`[CancellationTracker] Recording completion - Key: ${key}, Count: ${completionCount}, Total: ${tracking.completedCancellations}/${tracking.totalBids} (${percentage}%)`);
      }
    } else {
      console.log(`[CancellationTracker] WARNING: No tracking found for key: ${key}`);
    }
  }

  /**
   * Get current cancellation percentage
   * @returns Percentage (0-100) or null if not tracking
   */
  getPercentage(taskId: string, marketplace?: string): number | null {
    const key = this.getKey(taskId, marketplace);
    const tracking = this.trackingMap.get(key);
    
    console.log(`[CancellationTracker] getPercentage called for key: ${key}`);
    
    if (!tracking) {
      console.log(`[CancellationTracker] No tracking data for key: ${key}`);
      return null;
    }

    if (tracking.totalBids === 0) {
      console.log(`[CancellationTracker] Total bids is 0 for key: ${key}, returning 100%`);
      return 100; // No bids to cancel = 100% complete
    }

    // Calculate percentage with 2 decimal places
    const percentage = (tracking.completedCancellations / tracking.totalBids) * 100;
    const result = Math.min(100, parseFloat(percentage.toFixed(2)));
    
    console.log(`[CancellationTracker] Percentage for key ${key}: ${result}% (${tracking.completedCancellations}/${tracking.totalBids})`);
    
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
    
    if (taskLevelTracking) {
      // We have task-level tracking, calculate from marketplace completions
      let totalCompleted = 0;
      const marketplaces = ['opensea', 'magiceden', 'blur'];
      
      // Sum up completions from each marketplace
      marketplaces.forEach(marketplace => {
        const marketplaceKey = this.getKey(taskId, marketplace);
        const marketplaceTracking = this.trackingMap.get(marketplaceKey);
        if (marketplaceTracking) {
          totalCompleted += marketplaceTracking.completedCancellations;
        }
      });
      
      // Calculate percentage based on task-level total
      if (taskLevelTracking.totalBids === 0) {
        return 100;
      }
      
      const percentage = (totalCompleted / taskLevelTracking.totalBids) * 100;
      return Math.min(100, parseFloat(percentage.toFixed(2)));
    }
    
    // No task-level tracking, return null
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
    this.trackingMap.delete(key);
    
    // Clear marketplace totals cache if clearing task-level tracking
    if (!marketplace) {
      const marketplaces = ['opensea', 'magiceden', 'blur'];
      marketplaces.forEach(mp => {
        const cacheKey = `${taskId}-${mp}-total`;
        this.marketplaceTotalsCache.delete(cacheKey);
      });
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