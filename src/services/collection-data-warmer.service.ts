/**
 * Collection Data Warmer Service
 *
 * Warms collection floor prices + best offers for all marketplaces
 * Uses LRU caching with reference counting for efficient warming
 *
 * Key Features:
 * - Warms all task collections at startup (100 tasks → 30 unique = 70% deduplication)
 * - Reference counting: collections stay warm while any task uses them
 * - Background warming every 4 minutes (Priority 999 - no bidding impact)
 * - Modular marketplace support via MARKETPLACES config
 */

import { LRUCache } from 'lru-cache';
import { getCachedFloorPrice, getCachedCollectionDetails, MARKETPLACES } from '../utils/collection-cache';
import { debugLog } from '../utils/debug-file-logger';
import { scheduleWithPriority } from '../init';

/**
 * Collection Data Warmer Service
 * Warms floor prices and best offers for all marketplaces
 */
class CollectionDataWarmerService {
  // Track collections that should be kept warm
  private warmingSlugs = new LRUCache<string, boolean>({
    max: 1000,                  // Max 1000 collections to warm (standardized minimum)
    ttl: 24 * 60 * 60 * 1000,  // 24 hours (keep collections warm longer)
    updateAgeOnGet: true,       // Extend TTL on each activity
    allowStale: false
  });

  // Track how many tasks reference each collection (reference counting)
  private collectionTaskCounts = new Map<string, Set<string>>(); // slug → Set<taskId>

  // Background warming properties
  private backgroundWarmingInterval: NodeJS.Timeout | null = null;
  private readonly WARMING_INTERVAL = 4 * 60 * 1000; // 4 minutes

  /**
   * Extract unique collections from tasks for efficient warming
   * Handles deduplication: 100 tasks → 30 unique collections
   */
  private extractUniqueCollections(tasks: Array<{ contract: { slug: string } }>): Set<string> {
    const uniqueSlugs = new Set<string>();

    for (const task of tasks) {
      if (task.contract?.slug) {
        uniqueSlugs.add(task.contract.slug.toLowerCase());
      }
    }

    return uniqueSlugs;
  }

  /**
   * Warm all task collections at startup
   * Deduplicates automatically: 100 tasks with 30 unique collections → warms 30
   *
   * @param tasks Array of all tasks loaded from database
   */
  warmAllTaskCollections(tasks: Array<{ _id: string; contract: { slug: string } }>): void {
    const uniqueSlugs = this.extractUniqueCollections(tasks);

    debugLog('CollectionDataWarmer',
      `[INIT] Warming all task collections | totalTasks=${tasks.length} | uniqueCollections=${uniqueSlugs.size}`
    );

    // Add all unique slugs to warming set
    for (const slug of uniqueSlugs) {
      this.warmingSlugs.set(slug, true);

      // Trigger immediate first warm for each collection
      getCachedCollectionDetails(slug)
        .then(details => {
          const contractAddress = details?.contracts?.[0]?.address;

          // Warm all marketplaces using modular config
          const warmPromises = Object.entries(MARKETPLACES).map(([, config]) => {
            const cacheKey = config.getKeyFromSlug(slug, contractAddress);
            if (!cacheKey) return Promise.resolve();

            return Promise.allSettled([
              config.fetchFloor(cacheKey).then(floor => config.floorCache.set(cacheKey, floor)),
              config.fetchBestOffer(cacheKey).then(offer => config.bestOfferCache.set(cacheKey, offer))
            ]);
          });

          return Promise.allSettled(warmPromises);
        })
        .catch(() => { /* Best-effort warming */ });
    }

    debugLog('CollectionDataWarmer',
      `[INIT] Started warming | collections=${uniqueSlugs.size}`
    );
  }

  /**
   * Start warming a collection with reference counting
   * Warms floors + offers for all marketplaces using modular config
   */
  startWarming(slug: string, taskId: string): void {
    const key = slug.toLowerCase();

    // Add to reference count
    if (!this.collectionTaskCounts.has(key)) {
      this.collectionTaskCounts.set(key, new Set());
    }
    this.collectionTaskCounts.get(key)!.add(taskId);

    // Only start warming if first task for this collection
    if (this.collectionTaskCounts.get(key)!.size === 1) {
      this.warmingSlugs.set(key, true);

      // Immediate first warm (floor + offers) using modular approach
      getCachedCollectionDetails(slug)
        .then(details => {
          const contractAddress = details?.contracts?.[0]?.address;

          // Warm all marketplaces using config
          const warmPromises = Object.entries(MARKETPLACES).map(([, config]) => {
            const cacheKey = config.getKeyFromSlug(slug, contractAddress);
            if (!cacheKey) return Promise.resolve();

            return Promise.allSettled([
              config.fetchFloor(cacheKey).then(floor => config.floorCache.set(cacheKey, floor)),
              config.fetchBestOffer(cacheKey).then(offer => config.bestOfferCache.set(cacheKey, offer))
            ]);
          });

          return Promise.allSettled(warmPromises);
        })
        .catch(() => { /* Best-effort warming */ });

      debugLog('CollectionDataWarmer', `[START] First task for collection | slug=${slug} | taskId=${taskId}`);
    } else {
      debugLog('CollectionDataWarmer', `[REF++] Additional task for collection | slug=${slug} | taskId=${taskId} | totalTasks=${this.collectionTaskCounts.get(key)!.size}`);
    }
  }

  /**
   * Stop warming a collection with reference counting
   * Only stops when no more tasks reference this collection
   */
  stopWarming(slug: string, taskId: string): void {
    const key = slug.toLowerCase();

    // Remove from reference count
    const taskSet = this.collectionTaskCounts.get(key);
    if (taskSet) {
      taskSet.delete(taskId);

      // Only stop warming if no more tasks reference this collection
      if (taskSet.size === 0) {
        this.warmingSlugs.delete(key);
        this.collectionTaskCounts.delete(key);
        debugLog('CollectionDataWarmer', `[STOP] Last task stopped | slug=${slug} | taskId=${taskId}`);
      } else {
        debugLog('CollectionDataWarmer', `[REF--] Task stopped, collection still in use | slug=${slug} | taskId=${taskId} | remainingTasks=${taskSet.size}`);
      }
    }
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

      debugLog('CollectionDataWarmer', `[ACTIVITY] Refreshed | slug=${slug}`);
    }
  }

  /**
   * Get warmer statistics
   */
  getStats() {
    return {
      activeWarmingCollections: this.warmingSlugs.size,
      totalTaskReferences: Array.from(this.collectionTaskCounts.values()).reduce((sum, set) => sum + set.size, 0),
      collections: Array.from(this.collectionTaskCounts.entries()).map(([slug, tasks]) => ({
        slug,
        taskCount: tasks.size
      }))
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

    debugLog('CollectionDataWarmer', '[BG-START] Background warming started (4min interval)');
  }

  /**
   * Perform a background warming pass for all active collections
   * Warms floors + offers for all marketplaces using modular config
   */
  private async performBackgroundWarmingPass(): Promise<void> {
    const slugs = Array.from(this.warmingSlugs.keys());
    if (slugs.length === 0) return;

    const startTime = Date.now();
    const stats = {
      floors: { success: 0, error: 0 },
      offers: { success: 0, error: 0 }
    };

    debugLog('CollectionDataWarmer', `[BG-PASS] Starting warming pass | collections=${slugs.length}`);

    for (const slug of slugs) {
      try {
        // Get contract address (needed for Blur/MagicEden)
        const details = await scheduleWithPriority(() => getCachedCollectionDetails(slug), 999)
          .catch(() => null);
        const contractAddress = details?.contracts?.[0]?.address;

        // Warm floors for all marketplaces (using modular config)
        const floorPromises = Object.entries(MARKETPLACES).map(([name, config]) => {
          const key = config.getKeyFromSlug(slug, contractAddress);
          if (!key) return Promise.reject(`Missing key for ${name}`);

          return scheduleWithPriority(() => config.fetchFloor(key), 999)
            .then(floor => {
              config.floorCache.set(key, floor);
              stats.floors.success++;
            })
            .catch(() => stats.floors.error++);
        });

        // Warm best offers for all marketplaces
        const offerPromises = Object.entries(MARKETPLACES).map(([name, config]) => {
          const key = config.getKeyFromSlug(slug, contractAddress);
          if (!key) return Promise.reject(`Missing key for ${name}`);

          return scheduleWithPriority(() => config.fetchBestOffer(key), 999)
            .then(offer => {
              config.bestOfferCache.set(key, offer);
              stats.offers.success++;
            })
            .catch(() => stats.offers.error++);
        });

        // Execute all warmings in parallel
        await Promise.allSettled([...floorPromises, ...offerPromises]);

      } catch {
        stats.floors.error++;
        stats.offers.error++;
      }
    }

    const elapsed = Date.now() - startTime;
    debugLog('CollectionDataWarmer',
      `[BG-PASS] Completed | floors=${stats.floors.success}/${stats.floors.success + stats.floors.error} | ` +
      `offers=${stats.offers.success}/${stats.offers.success + stats.offers.error} | elapsedMs=${elapsed}`
    );
  }

  /**
   * Stop background warming job
   */
  stopBackgroundWarming(): void {
    if (this.backgroundWarmingInterval) {
      clearInterval(this.backgroundWarmingInterval);
      this.backgroundWarmingInterval = null;
      debugLog('CollectionDataWarmer', '[BG-STOP] Background warming stopped');
    }
  }

  /**
   * Shutdown (cleanup)
   */
  shutdown(): void {
    this.stopBackgroundWarming();
    this.warmingSlugs.clear();
    this.collectionTaskCounts.clear();
    debugLog('CollectionDataWarmer', '[SHUTDOWN] Cleared all warming tasks');
  }
}

// Singleton instance
export const collectionDataWarmer = new CollectionDataWarmerService();
