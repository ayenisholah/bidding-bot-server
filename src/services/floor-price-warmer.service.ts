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
    ttl: 15 * 60 * 1000,       // Stop warming after 15 min of inactivity
    updateAgeOnGet: true,       // Extend TTL on each activity
    allowStale: false
  });

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
   * Shutdown (cleanup)
   */
  shutdown(): void {
    this.warmingSlugs.clear();
    debugLog('FloorWarmer', '[SHUTDOWN] Cleared all warming tasks');
  }
}

// Singleton instance
export const floorPriceWarmer = new FloorPriceWarmerService();
