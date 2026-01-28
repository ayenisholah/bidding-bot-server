import { LRUCache } from 'lru-cache';
import { debugLog } from './debug-file-logger';

export interface BidLogEntry {
  taskId: string;
  title: string;
  message: string;
  messageHash?: string;
  type: 'skipped' | 'warning' | 'error';
  marketplace: 'opensea' | 'magiceden' | 'blur' | 'system';
  timestamp: Date;
}

interface LogsCacheStats {
  hits: number;
  misses: number;
  sets: number;
  deletes: number;
  totalRequests: number;
  hitRate: string;
  size: number;
  maxSize: number;
}

class LogsCache {
  // Cache: key = taskId-messageHash-marketplace
  private cache: LRUCache<string, BidLogEntry>;

  // Stats tracking
  private stats = {
    hits: 0,
    misses: 0,
    sets: 0,
    deletes: 0,
    totalRequests: 0
  };

  // Task TTL mapping (taskId → TTL in ms)
  private taskTTLs = new Map<string, number>();

  constructor() {
    // Start with default 24-hour TTL
    // Will be updated per-task when task starts
    this.cache = new LRUCache<string, BidLogEntry>({
      max: 100000, // Large enough for all tasks
      ttl: 24 * 60 * 60 * 1000, // Default 24 hours
      ttlAutopurge: true,
      updateAgeOnGet: false // Don't refresh TTL on reads
    });

    // Log stats every 5 minutes
    setInterval(() => this.logStats(), 300000);
  }

  /**
   * Set TTL for a specific task (based on task bid duration)
   */
  setTaskTTL(taskId: string, durationHours: number): void {
    const ttlMs = durationHours * 60 * 60 * 1000;
    this.taskTTLs.set(taskId, ttlMs);
    debugLog('LogsCache', `Set TTL for task ${taskId}: ${durationHours}h (${ttlMs}ms)`);
  }

  /**
   * Get cache key for a log entry
   */
  private getCacheKey(taskId: string, messageHash: string, marketplace: string): string {
    return `${taskId}-${messageHash}-${marketplace}`;
  }

  /**
   * Find one log entry (for deduplication)
   */
  async findOne(
    taskId: string,
    messageHash: string,
    marketplace: string
  ): Promise<BidLogEntry | null> {
    this.stats.totalRequests++;
    const key = this.getCacheKey(taskId, messageHash, marketplace);

    const entry = this.cache.get(key);
    if (entry) {
      this.stats.hits++;
      debugLog('LogsCache', `CACHE_HIT | ${key}`);
      return entry;
    }

    this.stats.misses++;
    debugLog('LogsCache', `CACHE_MISS | ${key}`);
    return null;
  }

  /**
   * Create or update a log entry
   */
  async upsert(entry: BidLogEntry): Promise<void> {
    const key = this.getCacheKey(entry.taskId, entry.messageHash || '', entry.marketplace);

    // Get task-specific TTL or use default
    const ttl = this.taskTTLs.get(entry.taskId) || (24 * 60 * 60 * 1000);

    this.cache.set(key, entry, { ttl });
    this.stats.sets++;

    // CACHE_SET log removed - was logging every error cache operation (75MB/day)
    // Only cache initialization and errors need logging, not every SET
  }

  /**
   * Update log timestamp
   */
  async updateTimestamp(
    taskId: string,
    messageHash: string,
    marketplace: string,
    timestamp: Date
  ): Promise<void> {
    const key = this.getCacheKey(taskId, messageHash, marketplace);
    const existing = this.cache.get(key);

    if (existing) {
      existing.timestamp = timestamp;
      const ttl = this.taskTTLs.get(taskId) || (24 * 60 * 60 * 1000);
      this.cache.set(key, existing, { ttl });

      debugLog('LogsCache', `CACHE_UPDATE | ${key} | timestamp=${timestamp.toISOString()}`);
    }
  }

  /**
   * Delete all logs for a task (on task start)
   */
  async deleteMany(taskId: string): Promise<number> {
    let deletedCount = 0;

    // Iterate through cache and delete matching taskId entries
    for (const [key, entry] of this.cache.entries()) {
      if (entry.taskId === taskId) {
        this.cache.delete(key);
        deletedCount++;
      }
    }

    this.stats.deletes += deletedCount;
    debugLog('LogsCache', `CACHE_DELETE_MANY | taskId=${taskId} | deleted=${deletedCount}`);

    // Remove task TTL mapping
    this.taskTTLs.delete(taskId);

    return deletedCount;
  }

  /**
   * Find all logs for a task (for API retrieval)
   */
  async find(taskId: string): Promise<BidLogEntry[]> {
    const logs: BidLogEntry[] = [];

    // Iterate through cache and collect matching entries
    for (const entry of this.cache.values()) {
      if (entry.taskId === taskId) {
        logs.push(entry);
      }
    }

    // Sort by timestamp descending (newest first)
    logs.sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime());

    debugLog('LogsCache', `CACHE_FIND | taskId=${taskId} | found=${logs.length}`);

    return logs;
  }

  /**
   * Get cache statistics
   */
  getStats(): LogsCacheStats {
    const hitRate = this.stats.totalRequests > 0
      ? ((this.stats.hits / this.stats.totalRequests) * 100).toFixed(2) + '%'
      : '0%';

    return {
      hits: this.stats.hits,
      misses: this.stats.misses,
      sets: this.stats.sets,
      deletes: this.stats.deletes,
      totalRequests: this.stats.totalRequests,
      hitRate,
      size: this.cache.size,
      maxSize: this.cache.max
    };
  }

  /**
   * Log periodic stats
   */
  private logStats(): void {
    if (this.stats.totalRequests > 0) {
      const stats = this.getStats();
      debugLog('LogsCache',
        `[5min Stats] hits=${stats.hits} | misses=${stats.misses} | ` +
        `hitRate=${stats.hitRate} | sets=${stats.sets} | deletes=${stats.deletes} | ` +
        `size=${stats.size}/${stats.maxSize}`
      );
    }
  }
}

// Export singleton
export const logsCache = new LogsCache();

// Export stats function for health endpoint
export function getLogsCacheStats(): LogsCacheStats {
  return logsCache.getStats();
}
