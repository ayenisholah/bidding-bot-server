/**
 * Time-Bucketed Bid Rate Tracker
 *
 * Optimized bid rate tracking with O(1) operations using circular buffer.
 * Replaces the previous O(6n) array-based implementation.
 *
 * Key improvements:
 * - O(1) trackBid() instead of O(6n)
 * - Fixed memory footprint (160 bytes) vs unbounded array growth
 * - Zero GC pressure (no allocations per bid)
 * - Time bucketing groups bids by second
 */

export interface BidRateStats {
  bidsPerSecond: number;
  totalBids: number;
  windowPeriod: number;
}

export interface MarketplaceBidRates {
  opensea: BidRateStats;
  blur: BidRateStats;
  magiceden: BidRateStats;
}

/**
 * Circular buffer-based bid rate tracker for a single marketplace
 * Groups bids by second for O(1) insertion
 */
export class TimeBucketedBidRateTracker {
  private buckets: number[];        // Fixed-size circular buffer of bid counts
  private timestamps: number[];     // Corresponding timestamps (seconds)
  private head: number = 0;         // Write pointer (next position to write)
  private size: number = 0;         // Current number of valid buckets
  private totalBids: number = 0;    // Running sum for O(1) calculation
  private readonly capacity: number;

  /**
   * @param windowSeconds - Time window for tracking (default 10 seconds)
   */
  constructor(windowSeconds: number = 10) {
    this.capacity = windowSeconds;
    this.buckets = new Array(windowSeconds).fill(0);
    this.timestamps = new Array(windowSeconds).fill(0);
  }

  /**
   * Track a bid - O(1) operation
   * Groups bids by second, no array reconstruction
   *
   * @param timestamp - Unix timestamp in seconds (defaults to now)
   */
  trackBid(timestamp: number = Math.floor(Date.now() / 1000)): void {
    // Get the most recently written bucket
    const currentHead = (this.head - 1 + this.capacity) % this.capacity;

    // Same second? Increment existing bucket (O(1))
    if (this.size > 0 && this.timestamps[currentHead] === timestamp) {
      this.buckets[currentHead]++;
      this.totalBids++;
      return; // Early exit - no cleanup needed
    }

    // New second - advance circular buffer
    if (this.size === this.capacity) {
      // Buffer is full - overwrite oldest bucket
      this.totalBids -= this.buckets[this.head];
    } else {
      this.size++;
    }

    // Write new bucket
    this.buckets[this.head] = 1;
    this.timestamps[this.head] = timestamp;
    this.head = (this.head + 1) % this.capacity;
    this.totalBids++;
  }

  /**
   * Calculate bids per second - O(1) with lazy cleanup
   *
   * Does not modify the buffer - just counts valid buckets on read.
   * This allows getBidsPerSecond() to be called frequently without
   * triggering expensive cleanup operations.
   *
   * @param now - Current timestamp in seconds
   * @returns Bid rate statistics
   */
  getBidsPerSecond(now: number = Math.floor(Date.now() / 1000)): BidRateStats {
    const cutoff = now - this.capacity;
    let validBids = this.totalBids;
    let validSize = this.size;
    let oldestValid = now;

    // Lazy cleanup: count valid buckets without modifying buffer
    // This is O(capacity) but capacity is fixed at 10, so effectively O(1)
    let index = this.head;
    for (let i = 0; i < this.size; i++) {
      index = (index - 1 + this.capacity) % this.capacity;
      const ts = this.timestamps[index];

      if (ts <= cutoff) {
        // Bucket is expired
        validBids -= this.buckets[index];
        validSize--;
      } else {
        // Track oldest valid timestamp
        oldestValid = Math.min(oldestValid, ts);
      }
    }

    // Calculate rate over actual time span
    const timeSpan = validSize > 0 ? Math.max(1, now - oldestValid + 1) : this.capacity;
    const bidsPerSecond = validBids / timeSpan;

    return {
      bidsPerSecond: parseFloat(bidsPerSecond.toFixed(2)),
      totalBids: validBids,
      windowPeriod: this.capacity
    };
  }

  /**
   * Reset all tracking state
   * Useful for testing or when task stops
   */
  reset(): void {
    this.buckets.fill(0);
    this.timestamps.fill(0);
    this.head = 0;
    this.size = 0;
    this.totalBids = 0;
  }

  /**
   * Get current buffer state (for debugging/testing)
   */
  getState() {
    return {
      buckets: [...this.buckets],
      timestamps: [...this.timestamps],
      head: this.head,
      size: this.size,
      totalBids: this.totalBids,
      capacity: this.capacity
    };
  }
}

/**
 * Wrapper for tracking bid rates across all marketplaces
 */
export class MarketplaceBidRateTracker {
  private trackers: Record<'opensea' | 'blur' | 'magiceden', TimeBucketedBidRateTracker>;

  constructor(windowSeconds: number = 10) {
    this.trackers = {
      opensea: new TimeBucketedBidRateTracker(windowSeconds),
      blur: new TimeBucketedBidRateTracker(windowSeconds),
      magiceden: new TimeBucketedBidRateTracker(windowSeconds)
    };
  }

  /**
   * Track a bid for a specific marketplace - O(1)
   */
  trackBid(marketplace: 'opensea' | 'blur' | 'magiceden', timestamp?: number): void {
    this.trackers[marketplace].trackBid(timestamp);
  }

  /**
   * Get bid rates for all marketplaces - O(1)
   */
  getAllBidRates(now?: number): MarketplaceBidRates {
    const timestamp = now ?? Math.floor(Date.now() / 1000);
    return {
      opensea: this.trackers.opensea.getBidsPerSecond(timestamp),
      blur: this.trackers.blur.getBidsPerSecond(timestamp),
      magiceden: this.trackers.magiceden.getBidsPerSecond(timestamp)
    };
  }

  /**
   * Get bid rate for a specific marketplace - O(1)
   */
  getBidRate(marketplace: 'opensea' | 'blur' | 'magiceden', now?: number): BidRateStats {
    const timestamp = now ?? Math.floor(Date.now() / 1000);
    return this.trackers[marketplace].getBidsPerSecond(timestamp);
  }

  /**
   * Reset all marketplace trackers
   */
  resetAll(): void {
    Object.values(this.trackers).forEach(tracker => tracker.reset());
  }

  /**
   * Reset specific marketplace tracker
   */
  reset(marketplace: 'opensea' | 'blur' | 'magiceden'): void {
    this.trackers[marketplace].reset();
  }
}
