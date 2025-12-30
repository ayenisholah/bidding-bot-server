/**
 * Partitioned Dispatcher for Cursor-Based Pull Architecture
 *
 * Dispatches events to per-contract/slug p-queues for controlled concurrency.
 * This ensures:
 * - Events for the same collection are processed sequentially
 * - Different collections can be processed in parallel
 * - Backpressure is managed at the partition level
 * - LRU eviction prevents unbounded partition growth
 */

import PQueue from 'p-queue';
import { NormalizedBidEvent } from './types';
import { BoundedCache } from '../utils/bounded-cache';
import { debugLog, warnLog, errorLog } from '../utils/debug-file-logger';

type PartitionKey = string; // contractAddress or slug
type EventHandler = (event: NormalizedBidEvent) => Promise<void>;
type TaskMatcher = (event: NormalizedBidEvent) => boolean;

interface DispatcherConfig {
  concurrencyPerPartition: number;  // Max concurrent handlers per partition
  maxPartitions: number;            // Max active partitions (LRU eviction)
  partitionIdleTimeoutMs: number;   // Remove idle partitions after this time
  handler: EventHandler;            // Event handler function
  taskMatcher: TaskMatcher;         // Filter events based on active tasks
}

interface PartitionState {
  queue: PQueue;
  lastActivity: number;
  eventCount: number;
}

interface DispatcherMetrics {
  eventsDispatched: number;
  eventsFiltered: number;
  eventsDeduplicated: number;
  partitionsCreated: number;
  partitionsEvicted: number;
  handlerExecuted: number;  // CP3: Handler successfully completed
  handlerFailed: number;    // CP3: Handler threw error
}

export class PartitionedDispatcher {
  private config: DispatcherConfig;
  private partitions: Map<PartitionKey, PartitionState> = new Map();
  private cleanupTimer: NodeJS.Timeout | null = null;

  // Deduplication cache (event already dispatched within 5s)
  private dispatchedEvents: BoundedCache<string, boolean>;

  // Metrics
  private metrics: DispatcherMetrics = {
    eventsDispatched: 0,
    eventsFiltered: 0,
    eventsDeduplicated: 0,
    partitionsCreated: 0,
    partitionsEvicted: 0,
    handlerExecuted: 0,
    handlerFailed: 0
  };

  constructor(config: Partial<DispatcherConfig> & { handler: EventHandler; taskMatcher: TaskMatcher }) {
    this.config = {
      concurrencyPerPartition: 3,
      maxPartitions: 1000,
      partitionIdleTimeoutMs: 60_000,
      ...config
    };

    this.dispatchedEvents = new BoundedCache(50_000, 5_000, 'dispatcher-dedup');

    // Start cleanup timer
    this.cleanupTimer = setInterval(() => this.cleanupIdlePartitions(), 30_000);
  }

  /**
   * Get or create partition queue for a given key
   */
  private getPartition(key: PartitionKey): PartitionState {
    let partition = this.partitions.get(key);

    if (!partition) {
      // Evict oldest partition if at capacity
      if (this.partitions.size >= this.config.maxPartitions) {
        this.evictOldestPartition();
      }

      partition = {
        queue: new PQueue({
          concurrency: this.config.concurrencyPerPartition,
          throwOnTimeout: true,
          timeout: 30_000
        }),
        lastActivity: Date.now(),
        eventCount: 0
      };

      this.partitions.set(key, partition);
      this.metrics.partitionsCreated++;
      debugLog('PullDispatcher', `Created partition: ${key}`);
    }

    return partition;
  }

  /**
   * Evict the least recently active partition
   */
  private evictOldestPartition(): void {
    let oldestKey: PartitionKey | null = null;
    let oldestTime = Infinity;

    for (const [key, state] of this.partitions) {
      // Don't evict partitions with pending work
      if (state.queue.pending > 0 || state.queue.size > 0) continue;

      if (state.lastActivity < oldestTime) {
        oldestTime = state.lastActivity;
        oldestKey = key;
      }
    }

    if (oldestKey) {
      this.partitions.delete(oldestKey);
      this.metrics.partitionsEvicted++;
      debugLog('PullDispatcher', `Evicted partition: ${oldestKey}`);
    }
  }

  /**
   * Cleanup idle partitions
   */
  private cleanupIdlePartitions(): void {
    const now = Date.now();
    const threshold = now - this.config.partitionIdleTimeoutMs;
    let cleaned = 0;

    for (const [key, state] of this.partitions) {
      // Skip partitions with pending work
      if (state.queue.pending > 0 || state.queue.size > 0) continue;

      if (state.lastActivity < threshold) {
        this.partitions.delete(key);
        cleaned++;
      }
    }

    if (cleaned > 0) {
      debugLog('PullDispatcher', `Cleaned up ${cleaned} idle partitions`);
    }
  }

  /**
   * Dispatch event to appropriate partition
   *
   * @param event - Normalized event to dispatch
   */
  async dispatch(event: NormalizedBidEvent): Promise<void> {
    // Early filter: Check if any task cares about this event
    if (!this.config.taskMatcher(event)) {
      this.metrics.eventsFiltered++;
      return;
    }

    // Deduplication: Skip if already dispatched recently
    const dedupKey = `${event.orderHash}-${event.seq}`;
    if (this.dispatchedEvents.has(dedupKey)) {
      this.metrics.eventsDeduplicated++;
      return;
    }
    this.dispatchedEvents.set(dedupKey, true);

    // Determine partition key (prefer contract, fallback to slug)
    const partitionKey = event.contractAddress || event.slug;
    if (!partitionKey) {
      this.metrics.eventsFiltered++;
      return;
    }

    // Get or create partition
    const partition = this.getPartition(partitionKey);
    partition.lastActivity = Date.now();
    partition.eventCount++;

    // Add to partition queue (non-blocking)
    partition.queue.add(async () => {
      try {
        // CP3: Log before handler execution
        debugLog('PullDispatcher', `CP_DISPATCHED | seq=${event.seq} | marketplace=${event.marketplace} | partition=${partitionKey}`);

        await this.config.handler(event);

        // CP3: Track successful handler execution
        this.metrics.eventsDispatched++;
        this.metrics.handlerExecuted++;

        // Log checkpoint progress periodically
        if (this.metrics.handlerExecuted % 1000 === 0) {
          debugLog('PullDispatcher', `CP_EXECUTED | total=${this.metrics.handlerExecuted} | failed=${this.metrics.handlerFailed} | partitions=${this.partitions.size}`);
        }
      } catch (error) {
        this.metrics.handlerFailed++;
        errorLog('PullDispatcher', `Handler error for ${partitionKey} | seq=${event.seq}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }).catch(error => {
      errorLog('PullDispatcher', `Queue error for ${partitionKey}: ${error instanceof Error ? error.message : String(error)}`);
    });
  }

  /**
   * Get current metrics
   */
  getMetrics(): DispatcherMetrics & {
    activePartitions: number;
    totalPending: number;
    totalQueued: number;
  } {
    let totalPending = 0;
    let totalQueued = 0;

    for (const state of this.partitions.values()) {
      totalPending += state.queue.pending;
      totalQueued += state.queue.size;
    }

    return {
      ...this.metrics,
      activePartitions: this.partitions.size,
      totalPending,
      totalQueued
    };
  }

  /**
   * Get queue depth for backpressure monitoring
   */
  getTotalQueueDepth(): number {
    let total = 0;
    for (const state of this.partitions.values()) {
      total += state.queue.pending + state.queue.size;
    }
    return total;
  }

  /**
   * Wait for all partitions to drain
   */
  async drain(): Promise<void> {
    const drainPromises = Array.from(this.partitions.values())
      .map(state => state.queue.onIdle());

    await Promise.all(drainPromises);
  }

  /**
   * Shutdown dispatcher
   */
  async shutdown(): Promise<void> {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }

    // Pause all partitions
    for (const state of this.partitions.values()) {
      state.queue.pause();
    }

    // Wait for pending work
    await this.drain();

    this.partitions.clear();
    debugLog('PullDispatcher', 'Dispatcher shutdown complete');
  }

  /**
   * Update handler function (for hot-swapping)
   */
  setHandler(handler: EventHandler): void {
    this.config.handler = handler;
  }

  /**
   * Update task matcher function (for hot-swapping)
   */
  setTaskMatcher(taskMatcher: TaskMatcher): void {
    this.config.taskMatcher = taskMatcher;
  }
}
