import TaskRuntimeState, {
  MarketplaceCounts,
  MarketplaceTokenIds,
  MarketplaceTraits,
  MarketplaceBooleans
} from "../models/taskRuntimeState.model";
import { debugLog, warnLog, errorLog } from "./debug-file-logger";

/**
 * In-memory runtime state for a task
 * This is the same structure used in index.ts but consolidated
 */
export interface TaskRuntimeData {
  bidStats: MarketplaceCounts;
  skipStats: MarketplaceCounts;
  errorStats: MarketplaceCounts;
  floorPrices: MarketplaceCounts;
  bestOffers: MarketplaceCounts;
  currentTokenIds: MarketplaceTokenIds;
  currentTraits: MarketplaceTraits;
  warningBids: MarketplaceBooleans;
  dirty: boolean;  // Track if data needs to be flushed to MongoDB
  lastAccess: number;
}

const DEFAULT_COUNTS: MarketplaceCounts = { opensea: 0, magiceden: 0, blur: 0 };
const DEFAULT_BOOLEANS: MarketplaceBooleans = { opensea: false, magiceden: false, blur: false };

function createDefaultRuntimeData(): TaskRuntimeData {
  return {
    bidStats: { ...DEFAULT_COUNTS },
    skipStats: { ...DEFAULT_COUNTS },
    errorStats: { ...DEFAULT_COUNTS },
    floorPrices: { ...DEFAULT_COUNTS },
    bestOffers: { ...DEFAULT_COUNTS },
    currentTokenIds: { opensea: [], magiceden: [] },
    currentTraits: { opensea: {}, magiceden: {}, blur: {} },
    warningBids: { ...DEFAULT_BOOLEANS },
    dirty: false,
    lastAccess: Date.now()
  };
}

/**
 * LRU Cache for task runtime state with MongoDB persistence
 */
class TaskRuntimeCache {
  private cache = new Map<string, TaskRuntimeData>();
  private readonly maxSize: number;
  private flushInterval: NodeJS.Timeout | null = null;
  private readonly FLUSH_INTERVAL_MS = 30000; // Flush dirty entries every 30s

  constructor(maxSize: number = 100) {
    this.maxSize = maxSize;
    this.startFlushInterval();
  }

  private startFlushInterval(): void {
    this.flushInterval = setInterval(() => {
      this.flushDirtyEntries().catch(err => {
        errorLog('RuntimeCache', 'Flush interval error', err);
      });
    }, this.FLUSH_INTERVAL_MS);
  }

  /**
   * Get runtime data for a task (from cache or MongoDB)
   */
  async get(taskId: string): Promise<TaskRuntimeData> {
    // Check cache first
    const cached = this.cache.get(taskId);
    if (cached) {
      cached.lastAccess = Date.now();
      return cached;
    }

    // Try to load from MongoDB
    try {
      const doc = await TaskRuntimeState.findOneAndUpdate(
        { taskId },
        { $set: { accessedAt: new Date() } },
        { new: true }
      );

      if (doc) {
        const data: TaskRuntimeData = {
          bidStats: doc.bidStats || { ...DEFAULT_COUNTS },
          skipStats: doc.skipStats || { ...DEFAULT_COUNTS },
          errorStats: doc.errorStats || { ...DEFAULT_COUNTS },
          floorPrices: doc.floorPrices || { ...DEFAULT_COUNTS },
          bestOffers: doc.bestOffers || { ...DEFAULT_COUNTS },
          currentTokenIds: doc.currentTokenIds || { opensea: [], magiceden: [] },
          currentTraits: doc.currentTraits || { opensea: {}, magiceden: {}, blur: {} },
          warningBids: doc.warningBids || { ...DEFAULT_BOOLEANS },
          dirty: false,
          lastAccess: Date.now()
        };

        this.addToCache(taskId, data);
        return data;
      }
    } catch (err) {
      warnLog('RuntimeCache', `Failed to load from MongoDB for task ${taskId}`, { error: err instanceof Error ? err.message : err });
    }

    // Create new default state
    const newData = createDefaultRuntimeData();
    this.addToCache(taskId, newData);
    return newData;
  }

  /**
   * Get synchronously from cache only (for hot path operations)
   * Returns undefined if not in cache
   */
  getSync(taskId: string): TaskRuntimeData | undefined {
    const cached = this.cache.get(taskId);
    if (cached) {
      cached.lastAccess = Date.now();
    }
    return cached;
  }

  /**
   * Ensure task is loaded into cache (call during task start)
   */
  async preload(taskId: string): Promise<void> {
    await this.get(taskId);
  }

  /**
   * Mark data as dirty (needs to be flushed)
   */
  markDirty(taskId: string): void {
    const cached = this.cache.get(taskId);
    if (cached) {
      cached.dirty = true;
    }
  }

  /**
   * Delete task data from cache and MongoDB
   */
  async delete(taskId: string): Promise<void> {
    this.cache.delete(taskId);
    try {
      await TaskRuntimeState.deleteOne({ taskId });
    } catch (err) {
      warnLog('RuntimeCache', `Failed to delete from MongoDB for task ${taskId}`, { error: err instanceof Error ? err.message : err });
    }
  }

  /**
   * Flush all dirty entries to MongoDB
   */
  async flushDirtyEntries(): Promise<void> {
    const dirtyEntries: Array<{ taskId: string; data: TaskRuntimeData }> = [];

    for (const [taskId, data] of this.cache) {
      if (data.dirty) {
        dirtyEntries.push({ taskId, data });
      }
    }

    if (dirtyEntries.length === 0) return;

    debugLog('RuntimeCache', `Flushing ${dirtyEntries.length} dirty entries to MongoDB`);

    // Batch upsert to MongoDB
    const bulkOps = dirtyEntries.map(({ taskId, data }) => ({
      updateOne: {
        filter: { taskId },
        update: {
          $set: {
            bidStats: data.bidStats,
            skipStats: data.skipStats,
            errorStats: data.errorStats,
            floorPrices: data.floorPrices,
            bestOffers: data.bestOffers,
            currentTokenIds: data.currentTokenIds,
            currentTraits: data.currentTraits,
            warningBids: data.warningBids,
            accessedAt: new Date()
          }
        },
        upsert: true
      }
    }));

    try {
      await TaskRuntimeState.bulkWrite(bulkOps, { ordered: false });

      // Mark as clean
      for (const { taskId } of dirtyEntries) {
        const cached = this.cache.get(taskId);
        if (cached) {
          cached.dirty = false;
        }
      }
    } catch (err) {
      errorLog('RuntimeCache', 'Failed to flush dirty entries', err);
    }
  }

  /**
   * Force flush a specific task immediately
   */
  async flush(taskId: string): Promise<void> {
    const data = this.cache.get(taskId);
    if (!data || !data.dirty) return;

    try {
      await TaskRuntimeState.updateOne(
        { taskId },
        {
          $set: {
            bidStats: data.bidStats,
            skipStats: data.skipStats,
            errorStats: data.errorStats,
            floorPrices: data.floorPrices,
            bestOffers: data.bestOffers,
            currentTokenIds: data.currentTokenIds,
            currentTraits: data.currentTraits,
            warningBids: data.warningBids,
            accessedAt: new Date()
          }
        },
        { upsert: true }
      );
      data.dirty = false;
    } catch (err) {
      errorLog('RuntimeCache', `Failed to flush task ${taskId}`, err);
    }
  }

  /**
   * Add entry to cache with LRU eviction
   */
  private addToCache(taskId: string, data: TaskRuntimeData): void {
    // Evict if at capacity
    if (this.cache.size >= this.maxSize) {
      this.evictLRU();
    }
    this.cache.set(taskId, data);
  }

  /**
   * Evict least recently used entries
   */
  private evictLRU(): void {
    const evictCount = Math.ceil(this.maxSize * 0.1); // Evict 10%
    const entries = Array.from(this.cache.entries())
      .sort((a, b) => a[1].lastAccess - b[1].lastAccess);

    for (let i = 0; i < evictCount && i < entries.length; i++) {
      const [taskId, data] = entries[i];

      // Flush dirty entries before eviction
      if (data.dirty) {
        this.flush(taskId).catch(err => {
          errorLog('RuntimeCache', `Failed to flush during eviction for ${taskId}`, err);
        });
      }

      this.cache.delete(taskId);
    }

    debugLog('RuntimeCache', `Evicted ${evictCount} LRU entries, cache size now ${this.cache.size}`);
  }

  /**
   * Clear all cache entries (flush dirty ones first)
   */
  async clear(): Promise<void> {
    await this.flushDirtyEntries();
    this.cache.clear();
  }

  /**
   * Get cache statistics
   */
  getStats(): { size: number; maxSize: number; dirtyCount: number } {
    let dirtyCount = 0;
    for (const data of this.cache.values()) {
      if (data.dirty) dirtyCount++;
    }
    return { size: this.cache.size, maxSize: this.maxSize, dirtyCount };
  }

  /**
   * Stop the flush interval (for shutdown)
   */
  stop(): void {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
    }
  }
}

// Export singleton instance
export const taskRuntimeCache = new TaskRuntimeCache(100);

// Helper functions for common operations (maintain compatibility with existing code)

export async function getBidStats(taskId: string): Promise<MarketplaceCounts> {
  const data = await taskRuntimeCache.get(taskId);
  return data.bidStats;
}

export function getBidStatsSync(taskId: string): MarketplaceCounts | undefined {
  return taskRuntimeCache.getSync(taskId)?.bidStats;
}

export function setBidStat(taskId: string, marketplace: 'opensea' | 'magiceden' | 'blur', value: number): void {
  const data = taskRuntimeCache.getSync(taskId);
  if (data) {
    data.bidStats[marketplace] = value;
    taskRuntimeCache.markDirty(taskId);
  }
}

export function incrementBidStat(taskId: string, marketplace: 'opensea' | 'magiceden' | 'blur', amount: number = 1): void {
  const data = taskRuntimeCache.getSync(taskId);
  if (data) {
    data.bidStats[marketplace] += amount;
    taskRuntimeCache.markDirty(taskId);
  }
}

export async function getFloorPrice(taskId: string, marketplace: 'opensea' | 'magiceden' | 'blur'): Promise<number> {
  const data = await taskRuntimeCache.get(taskId);
  return data.floorPrices[marketplace];
}

export function setFloorPrice(taskId: string, marketplace: 'opensea' | 'magiceden' | 'blur', value: number): void {
  const data = taskRuntimeCache.getSync(taskId);
  if (data) {
    data.floorPrices[marketplace] = value;
    taskRuntimeCache.markDirty(taskId);
  }
}

export async function getBestOffer(taskId: string, marketplace: 'opensea' | 'magiceden' | 'blur'): Promise<number> {
  const data = await taskRuntimeCache.get(taskId);
  return data.bestOffers[marketplace];
}

export function setBestOffer(taskId: string, marketplace: 'opensea' | 'magiceden' | 'blur', value: number): void {
  const data = taskRuntimeCache.getSync(taskId);
  if (data) {
    data.bestOffers[marketplace] = value;
    taskRuntimeCache.markDirty(taskId);
  }
}

export function incrementSkipStat(taskId: string, marketplace: 'opensea' | 'magiceden' | 'blur', amount: number = 1): void {
  const data = taskRuntimeCache.getSync(taskId);
  if (data) {
    data.skipStats[marketplace] += amount;
    taskRuntimeCache.markDirty(taskId);
  }
}

export function incrementErrorStat(taskId: string, marketplace: 'opensea' | 'magiceden' | 'blur', amount: number = 1): void {
  const data = taskRuntimeCache.getSync(taskId);
  if (data) {
    data.errorStats[marketplace] += amount;
    taskRuntimeCache.markDirty(taskId);
  }
}

export function setWarningBid(taskId: string, marketplace: 'opensea' | 'magiceden' | 'blur', value: boolean): void {
  const data = taskRuntimeCache.getSync(taskId);
  if (data) {
    data.warningBids[marketplace] = value;
    taskRuntimeCache.markDirty(taskId);
  }
}

export async function deleteTaskRuntimeState(taskId: string): Promise<void> {
  await taskRuntimeCache.delete(taskId);
}

export async function preloadTaskRuntimeState(taskId: string): Promise<void> {
  await taskRuntimeCache.preload(taskId);
}
