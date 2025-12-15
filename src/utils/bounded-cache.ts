/**
 * BoundedCache: A memory-efficient cache with size limits and TTL support
 * Prevents unbounded memory growth in high-volume NFT marketplace data processing
 */
export class BoundedCache<K, V> {
  private cache = new Map<K, { value: V; timestamp: number }>();
  private maxSize: number;
  private ttl: number;
  private lastCleanup = Date.now();
  private cleanupInterval = 60000; // 1 minute

  constructor(maxSize = 10000, ttl = 30 * 60 * 1000) {
    this.maxSize = maxSize;
    this.ttl = ttl;
  }

  set(key: K, value: V): void {
    // Cleanup old entries if at capacity
    if (this.cache.size >= this.maxSize) {
      this.cleanup();
    }

    this.cache.set(key, {
      value,
      timestamp: Date.now()
    });

    // Periodic cleanup check
    if (Date.now() - this.lastCleanup > this.cleanupInterval) {
      this.cleanup();
    }
  }

  get(key: K): V | undefined {
    const entry = this.cache.get(key);
    if (!entry) return undefined;

    // Check if expired
    if (Date.now() - entry.timestamp > this.ttl) {
      this.cache.delete(key);
      return undefined;
    }

    return entry.value;
  }

  has(key: K): boolean {
    const value = this.get(key);
    return value !== undefined;
  }

  /**
   * Atomic check-and-add operation: adds the key-value pair only if the key doesn't already exist.
   * Returns true if the entry was added, false if it already existed.
   * This prevents race conditions in concurrent deduplication checks.
   */
  addIfNotExists(key: K, value: V): boolean {
    // Check if already exists (including expiration check)
    if (this.has(key)) {
      return false;  // Already exists, don't add
    }

    // Add the entry
    this.set(key, value);
    return true;  // Successfully added
  }

  delete(key: K): boolean {
    return this.cache.delete(key);
  }

  clear(): void {
    this.cache.clear();
  }

  get size(): number {
    return this.cache.size;
  }

  private cleanup(): void {
    const now = Date.now();

    // First pass: delete expired entries (no array copy needed)
    for (const [key, entry] of this.cache) {
      if (now - entry.timestamp > this.ttl) {
        this.cache.delete(key);
      }
    }

    // If still over capacity, delete oldest entries without sorting
    // Memory optimization: find and delete oldest entries one at a time instead of Array.from() + sort
    if (this.cache.size >= this.maxSize) {
      const targetDelete = Math.floor(this.maxSize * 0.2);
      let deleted = 0;

      while (deleted < targetDelete && this.cache.size > 0) {
        let oldestTimestamp = Infinity;
        let oldestKey: K | null = null;

        for (const [key, entry] of this.cache) {
          if (entry.timestamp < oldestTimestamp) {
            oldestTimestamp = entry.timestamp;
            oldestKey = key;
          }
        }

        if (oldestKey !== null) {
          this.cache.delete(oldestKey);
          deleted++;
        } else {
          break;
        }
      }
    }

    this.lastCleanup = now;
  }

  keys(): IterableIterator<K> {
    return this.cache.keys();
  }

  // Get statistics about the cache
  getStats(): {
    size: number;
    maxSize: number;
    ttl: number;
    oldestEntry: number | null;
    newestEntry: number | null;
  } {
    const entries = Array.from(this.cache.values());
    const timestamps = entries.map(e => e.timestamp);
    
    return {
      size: this.cache.size,
      maxSize: this.maxSize,
      ttl: this.ttl,
      oldestEntry: timestamps.length > 0 ? Math.min(...timestamps) : null,
      newestEntry: timestamps.length > 0 ? Math.max(...timestamps) : null
    };
  }
}