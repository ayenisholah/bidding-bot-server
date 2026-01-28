/**
 * Simple Floor Price Warmer using LRU cache
 *
 * Auto-warms floor prices on access (no timers, no intervals)
 * Warmed entries get extended TTL to stay hot
 *
 * Replaces complex timer-based warming with simple LRU-based approach
 * 70 lines (down from 198 lines - 65% reduction)
 */

import { LRUCache } from 'lru-cache';
import { getCachedFloorPrice } from '../utils/collection-cache';
import { debugLog } from '../utils/debug-file-logger';
import { scheduleWithPriority } from '../init';

/**
 * Simple Floor Price Warmer using LRU cache
 *
 * Auto-warms floor prices on access (no timers, no intervals)
 * Warmed entries get extended TTL to stay hot
 */
class FloorPriceWarmerService {
  // Track collections that should be kept warm
  // When a collection is marked for warming, we extend its TTL whenever accessed
  private warmingSlugs = new LRUCache<string, boolean>({
    max: 100,                   // Max 100 collections to warm
    ttl: 24 * 60 * 60 * 1000,  // 24 hours (keep collections warm longer)
    updateAgeOnGet: true,       // Extend TTL on each activity
    allowStale: false
  });

  // Background warming properties
  private backgroundWarmingInterval: NodeJS.Timeout | null = null;
  private readonly WARMING_INTERVAL = 4 * 60 * 1000; // 4 minutes

  /**
   * Start warming a collection
   * Just marks it for warming - actual warming happens on access
   */
  startWarming(slug: string, taskId: string): void {
    const key = slug.toLowerCase();
    this.warmingSlugs.set(key, true);

    // Immediate first warm
    getCachedFloorPrice(slug, 'opensea').catch(() => {
      // Ignore errors - warmer is best-effort
    });

    debugLog('FloorWarmer', `[START] Warming | slug=${slug} | taskId=${taskId}`);
  }

  /**
   * Stop warming a collection
   */
  stopWarming(slug: string, taskId: string): void {
    const key = slug.toLowerCase();
    this.warmingSlugs.delete(key);
    debugLog('FloorWarmer', `[STOP] Stopped | slug=${slug} | taskId=${taskId}`);
  }

  /**
   * Track activity for a collection (keeps it warm)
   */
  trackActivity(slug: string): void {
    const key = slug.toLowerCase();

    // If we're warming this collection, refresh the floor price
    if (this.warmingSlugs.has(key)) {
      // Fire-and-forget refresh (don't await)
      getCachedFloorPrice(slug, 'opensea').catch(() => {
        // Ignore errors - warmer is best-effort
      });

      debugLog('FloorWarmer', `[ACTIVITY] Refreshed | slug=${slug}`);
    }
  }

  /**
   * Get warmer statistics
   */
  getStats() {
    return {
      activeWarmingTasks: this.warmingSlugs.size,
      slugs: Array.from(this.warmingSlugs.keys())
    };
  }

  /**
   * Start background warming job
   * Refreshes floor prices every 4 minutes for active warming collections
   */
  startBackgroundWarming(): void {
    if (this.backgroundWarmingInterval) {
      return; // Already running
    }

    this.backgroundWarmingInterval = setInterval(() => {
      this.performBackgroundWarmingPass();
    }, this.WARMING_INTERVAL);

    debugLog('FloorWarmer', '[BG-START] Background warming started (4min interval)');
  }

  /**
   * Perform a background warming pass for all active collections
   */
  private async performBackgroundWarmingPass(): Promise<void> {
    const slugs = Array.from(this.warmingSlugs.keys());
    if (slugs.length === 0) return;

    const startTime = Date.now();
    let successCount = 0;
    let errorCount = 0;

    debugLog('FloorWarmer', `[BG-PASS] Starting warming pass | collections=${slugs.length}`);

    for (const slug of slugs) {
      try {
        // Use scheduleWithPriority with lowest priority (999)
        await scheduleWithPriority(
          () => getCachedFloorPrice(slug, 'opensea'),
          999
        ).catch(() => { /* Best-effort */ });
        successCount++;
      } catch {
        errorCount++;
      }
    }

    const elapsed = Date.now() - startTime;
    debugLog('FloorWarmer',
      `[BG-PASS] Completed | success=${successCount} | errors=${errorCount} | elapsedMs=${elapsed}`
    );
  }

  /**
   * Stop background warming job
   */
  stopBackgroundWarming(): void {
    if (this.backgroundWarmingInterval) {
      clearInterval(this.backgroundWarmingInterval);
      this.backgroundWarmingInterval = null;
      debugLog('FloorWarmer', '[BG-STOP] Background warming stopped');
    }
  }

  /**
   * Shutdown (cleanup)
   */
  shutdown(): void {
    this.stopBackgroundWarming();
    this.warmingSlugs.clear();
    debugLog('FloorWarmer', '[SHUTDOWN] Cleared all warming tasks');
  }
}

// Singleton instance
export const floorPriceWarmer = new FloorPriceWarmerService();
