/**
 * Pull Consumer Module for Cursor-Based Pull Architecture
 *
 * This module provides a complete pull-based event consumption system
 * that replaces the push-based WebSocket delivery from ws-proxy.
 *
 * Features:
 * - HTTP polling with cursor-based pagination
 * - Partitioned p-queues for per-collection concurrency control
 * - Backpressure monitoring and adaptive pull rate
 * - Gap detection and automatic cursor recovery
 * - Seamless integration with existing counterbid handlers
 */

import Redis from 'ioredis';
import { EventPuller } from './event-puller';
import { PartitionedDispatcher } from './partitioned-dispatcher';
import { BackpressureMonitor } from './backpressure';
import { createTaskMatcher, createEventHandler, createAdaptedHandlers } from './handler-adapter';
import { PullConsumerConfig, PullConsumerMetrics, NormalizedBidEvent } from './types';
import { infoLog, debugLog, warnLog } from '../utils/debug-file-logger';
import { ITask } from '../models/task.model';

// Re-export all components for external use
export { EventPuller } from './event-puller';
export { PartitionedDispatcher } from './partitioned-dispatcher';
export { BackpressureMonitor } from './backpressure';
export { CursorManager } from '../utils/cursor-manager';
export * from './types';
export * from './handler-adapter';

// Use the actual ITask type from the model for proper type compatibility
type CounterbidHandler = (data: any, task: ITask) => Promise<void>;

/**
 * Configuration for initializing the pull consumer
 */
export interface PullConsumerInitConfig {
  // ws-proxy URL (e.g., https://nfttools-ws-proxy.nfttools.io/)
  proxyUrl: string;

  // Redis instance for cursor persistence
  redis: Redis;

  // Pull interval in ms (default 50ms)
  pullIntervalMs?: number;

  // Max events per pull (default 1000)
  batchSize?: number;

  // Task lookup function (uses existing O(1) indexes)
  getRelevantTasks: (contractAddress?: string, slug?: string) => ITask[];

  // Existing counterbid handlers
  handleOpenseaCounterbid: CounterbidHandler;
  handleMagicEdenCounterbid: CounterbidHandler;
  handleBlurCounterbid: CounterbidHandler;
}

/**
 * PullConsumer orchestrates all pull-based event consumption components
 */
export class PullConsumer {
  private puller: EventPuller;
  private dispatcher: PartitionedDispatcher;
  private backpressureMonitor: BackpressureMonitor;
  private running: boolean = false;

  constructor(
    puller: EventPuller,
    dispatcher: PartitionedDispatcher,
    backpressureMonitor: BackpressureMonitor
  ) {
    this.puller = puller;
    this.dispatcher = dispatcher;
    this.backpressureMonitor = backpressureMonitor;
  }

  /**
   * Start the pull consumer
   */
  async start(): Promise<void> {
    if (this.running) {
      warnLog('PullConsumer', 'Already running');
      return;
    }

    infoLog('PullConsumer', 'Starting pull consumer...');

    // Start backpressure monitoring
    this.backpressureMonitor.start();

    // Start the pull loop
    await this.puller.start();

    this.running = true;
    infoLog('PullConsumer', 'Pull consumer started');
  }

  /**
   * Stop the pull consumer gracefully
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    infoLog('PullConsumer', 'Stopping pull consumer...');

    // Stop pulling first
    await this.puller.stop();

    // Stop backpressure monitor
    this.backpressureMonitor.stop();

    // Wait for dispatcher to drain
    await this.dispatcher.drain();

    // Shutdown dispatcher
    await this.dispatcher.shutdown();

    this.running = false;
    infoLog('PullConsumer', 'Pull consumer stopped');
  }

  /**
   * Check if running
   */
  isRunning(): boolean {
    return this.running;
  }

  /**
   * Get comprehensive metrics
   */
  getMetrics(): PullConsumerMetrics {
    const pullerMetrics = this.puller.getMetrics();
    const dispatcherMetrics = this.dispatcher.getMetrics();
    const backpressureMetrics = this.backpressureMonitor.getMetrics();

    return {
      puller: {
        cursor: pullerMetrics.cursor,
        pullCount: pullerMetrics.pullCount,
        eventsProcessed: pullerMetrics.eventsProcessed,
        gapsDetected: pullerMetrics.gapsDetected,
        errors: pullerMetrics.errors,
        lastPullLatencyMs: pullerMetrics.lastPullLatencyMs,
        avgPullLatencyMs: pullerMetrics.avgPullLatencyMs,
      },
      dispatcher: {
        eventsDispatched: dispatcherMetrics.eventsDispatched,
        eventsFiltered: dispatcherMetrics.eventsFiltered,
        eventsDeduplicated: dispatcherMetrics.eventsDeduplicated,
        activePartitions: dispatcherMetrics.activePartitions,
        totalPending: dispatcherMetrics.totalPending,
        totalQueued: dispatcherMetrics.totalQueued,
      },
      backpressure: {
        state: backpressureMetrics.state,
        utilization: backpressureMetrics.utilization,
      },
    };
  }

  /**
   * Force sync cursor to head (skip all current events)
   */
  async syncToHead(): Promise<void> {
    await this.puller.syncToHead();
  }

  /**
   * Get the event puller for direct access
   */
  getPuller(): EventPuller {
    return this.puller;
  }

  /**
   * Get the dispatcher for direct access
   */
  getDispatcher(): PartitionedDispatcher {
    return this.dispatcher;
  }
}

/**
 * Initialize the pull consumer with all required components
 *
 * This is the main factory function that wires everything together
 *
 * @param config - Configuration options
 * @returns Initialized PullConsumer ready to start
 */
export async function initializePullConsumer(
  config: PullConsumerInitConfig
): Promise<PullConsumer> {
  infoLog('PullConsumer', 'Initializing pull consumer...');

  // Create adapted handlers that work with normalized events
  const handlers = createAdaptedHandlers(
    config.handleOpenseaCounterbid,
    config.handleMagicEdenCounterbid,
    config.handleBlurCounterbid
  );

  // Create task matcher for filtering events
  const taskMatcher = createTaskMatcher(config.getRelevantTasks);

  // Create event handler that routes to marketplace handlers
  const eventHandler = createEventHandler(handlers, config.getRelevantTasks);

  // Create partitioned dispatcher
  const dispatcher = new PartitionedDispatcher({
    concurrencyPerPartition: 3,
    maxPartitions: 1000,
    partitionIdleTimeoutMs: 60_000,
    handler: eventHandler,
    taskMatcher: taskMatcher,
  });

  // Create backpressure monitor
  const backpressureMonitor = new BackpressureMonitor(dispatcher, {
    maxQueuedEvents: 10_000,
    maxPendingEvents: 1_000,
    slowdownThreshold: 0.7,
    pauseThreshold: 0.9,
    checkIntervalMs: 1000,
  });

  // Create event puller
  const puller = new EventPuller({
    proxyUrl: config.proxyUrl,
    pullIntervalMs: config.pullIntervalMs || 50,
    batchSize: config.batchSize || 1000,
    redis: config.redis,
    dispatcher: dispatcher,
    backpressureMonitor: backpressureMonitor,
  });

  infoLog('PullConsumer', `Initialized with proxyUrl=${config.proxyUrl}, interval=${config.pullIntervalMs || 50}ms, batch=${config.batchSize || 1000}`);

  return new PullConsumer(puller, dispatcher, backpressureMonitor);
}
