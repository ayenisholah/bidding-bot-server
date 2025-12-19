/**
 * BoundedCache: A memory-efficient cache with size limits and TTL support
 * Prevents unbounded memory growth in high-volume NFT marketplace data processing
 *
 * Fixes applied (2025-12-18):
 * - FIX #1: Truly atomic addIfNotExists using single Map lookup
 * - FIX #2: has() correctly handles falsy values (0, "", false, null)
 * - FIX #3: Single cleanup per set() instead of possible double cleanup
 * - FIX #4: O(n log n) cleanup instead of O(n²) for LRU eviction
 * - FIX #5: getStats() without full array copies - O(n) memory efficient
 * - FIX #6: keys() returns snapshot instead of live iterator
 */
import { debugLog, warnLog } from './debug-file-logger';

export class BoundedCache<K, V> {
  private cache = new Map<K, { value: V; timestamp: number }>();
  private maxSize: number;
  private ttl: number;
  private lastCleanup = Date.now();
  private cleanupInterval = 60000; // 1 minute
  private name: string; // For logging identification

  constructor(maxSize = 10000, ttl = 30 * 60 * 1000, name = 'unnamed') {
    this.maxSize = maxSize;
    this.ttl = ttl;
    this.name = name;
    debugLog('Cache', `[${this.name}] Initialized | maxSize=${maxSize} | ttl=${ttl}ms`);
  }

  /**
   * FIX #1: Truly atomic addIfNotExists
   * Uses single Map.get() to check existence AND expiration atomically.
   * No race window between check and set operations.
   */
  addIfNotExists(key: K, value: V): boolean {
    const keyStr = String(key).slice(0, 50);
    const now = Date.now();

    // Single atomic check for existence and expiration
    const existing = this.cache.get(key);
    if (existing !== undefined) {
      const age = now - existing.timestamp;
      if (age <= this.ttl) {
        // Key exists and not expired - BLOCKED
        debugLog('Cache', `[${this.name}] addIfNotExists BLOCKED | key=${keyStr} | age=${age}ms`);
        return false;
      }
      // Entry expired - will be replaced
      debugLog('Cache', `[${this.name}] addIfNotExists EXPIRED_REPLACE | key=${keyStr} | age=${age}ms`);
    }

    // Cleanup if at capacity (before adding)
    if (this.cache.size >= this.maxSize) {
      this.cleanup();
    }

    // Add entry atomically
    const sizeBefore = this.cache.size;
    this.cache.set(key, { value, timestamp: now });

    debugLog('Cache', `[${this.name}] addIfNotExists SUCCESS | key=${keyStr} | size=${sizeBefore}→${this.cache.size}`);
    return true;
  }

  /**
   * FIX #2: has() correctly handles falsy values
   * Checks if entry exists in cache (handles falsy values like 0, "", false, null)
   */
  has(key: K): boolean {
    const entry = this.cache.get(key);
    if (entry === undefined) return false; // Key doesn't exist

    // Check expiration
    if (Date.now() - entry.timestamp > this.ttl) {
      this.cache.delete(key);
      return false;
    }

    return true; // Key exists (even if value is falsy)
  }

  get(key: K): V | undefined {
    const entry = this.cache.get(key);
    if (!entry) return undefined;

    // Check if expired
    if (Date.now() - entry.timestamp > this.ttl) {
      this.cache.delete(key);
      debugLog('Cache', `[${this.name}] get EXPIRED | key=${String(key).slice(0, 50)}`);
      return undefined;
    }

    return entry.value;
  }

  /**
   * FIX #3: Single cleanup per set() instead of double cleanup
   * Consolidates capacity check and periodic cleanup into one condition
   */
  set(key: K, value: V): void {
    const now = Date.now();

    // Single cleanup check: either at capacity OR periodic interval
    const needsCleanup = this.cache.size >= this.maxSize ||
                         (now - this.lastCleanup > this.cleanupInterval);

    if (needsCleanup) {
      this.cleanup();
    }

    this.cache.set(key, { value, timestamp: now });
  }

  delete(key: K): boolean {
    const deleted = this.cache.delete(key);
    if (deleted) {
      debugLog('Cache', `[${this.name}] delete | key=${String(key).slice(0, 50)} | size=${this.cache.size}`);
    }
    return deleted;
  }

  clear(): void {
    const sizeBefore = this.cache.size;
    this.cache.clear();
    debugLog('Cache', `[${this.name}] clear | size=${sizeBefore}→0`);
  }

  get size(): number {
    return this.cache.size;
  }

  /**
   * FIX #4: O(n log n) cleanup instead of O(n²)
   * Previous implementation: O(n²) - iterated n times to find oldest for each delete
   * New implementation: O(n log n) - single sort, then delete in order
   */
  private cleanup(): void {
    const startTime = Date.now();
    const sizeBefore = this.cache.size;
    let expiredDeleted = 0;
    let lruDeleted = 0;

    // Pass 1: Delete expired entries - O(n)
    for (const [key, entry] of this.cache) {
      if (startTime - entry.timestamp > this.ttl) {
        this.cache.delete(key);
        expiredDeleted++;
      }
    }

    // Pass 2: If still over capacity, delete oldest 20% - O(n log n)
    if (this.cache.size >= this.maxSize) {
      const targetDelete = Math.floor(this.maxSize * 0.2);

      // Collect entries with timestamps - O(n) space, O(n) time
      const entries: Array<{ key: K; timestamp: number }> = [];
      for (const [key, entry] of this.cache) {
        entries.push({ key, timestamp: entry.timestamp });
      }

      // Sort by timestamp ascending (oldest first) - O(n log n)
      entries.sort((a, b) => a.timestamp - b.timestamp);

      // Delete oldest entries - O(targetDelete)
      for (let i = 0; i < targetDelete && i < entries.length; i++) {
        this.cache.delete(entries[i].key);
        lruDeleted++;
      }
    }

    this.lastCleanup = startTime;
    const duration = Date.now() - startTime;

    // Log cleanup stats if anything was deleted
    if (expiredDeleted > 0 || lruDeleted > 0) {
      debugLog('Cache', `[${this.name}] cleanup | expired=${expiredDeleted} | lru=${lruDeleted} | size=${sizeBefore}→${this.cache.size} | duration=${duration}ms`);
    }

    // Warn if cleanup took too long (>100ms indicates potential performance issue)
    if (duration > 100) {
      warnLog('Cache', `[${this.name}] cleanup SLOW | duration=${duration}ms | size=${this.cache.size}`);
    }
  }

  /**
   * FIX #6: keys() returns snapshot instead of live iterator
   * Returns array copy so modifications during iteration are safe
   */
  keys(): K[] {
    return Array.from(this.cache.keys());
  }

  /**
   * FIX #5: getStats() without full array copies
   * Iterates once to find min/max timestamps instead of creating array copies
   */
  getStats(): {
    size: number;
    maxSize: number;
    ttl: number;
    oldestEntry: number | null;
    newestEntry: number | null;
  } {
    let oldest = Infinity;
    let newest = 0;

    // Single pass to find min/max - O(n) time, O(1) space
    for (const entry of this.cache.values()) {
      if (entry.timestamp < oldest) oldest = entry.timestamp;
      if (entry.timestamp > newest) newest = entry.timestamp;
    }

    return {
      size: this.cache.size,
      maxSize: this.maxSize,
      ttl: this.ttl,
      oldestEntry: this.cache.size > 0 ? oldest : null,
      newestEntry: this.cache.size > 0 ? newest : null
    };
  }
}
