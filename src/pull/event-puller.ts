/**
 * Event Puller for Cursor-Based Pull Architecture
 *
 * The event puller is responsible for:
 * - Polling ws-proxy for new events at regular intervals
 * - Managing cursor position and persistence
 * - Handling gap detection and recovery
 * - Dispatching events to the partitioned dispatcher
 * - Adapting pull rate based on backpressure
 */

import axios, { AxiosInstance } from 'axios';
import Redis from 'ioredis';
import { CursorManager } from '../utils/cursor-manager';
import { PartitionedDispatcher } from './partitioned-dispatcher';
import { BackpressureMonitor } from './backpressure';
import { NormalizedBidEvent, PullResponse, HeadResponse } from './types';
import { debugLog, infoLog, warnLog, errorLog } from '../utils/debug-file-logger';

interface EventPullerConfig {
  proxyUrl: string;           // ws-proxy URL
  pullIntervalMs: number;     // How often to pull (default 50ms)
  batchSize: number;          // Max events per pull (default 1000)
  redis: Redis;               // Local Redis instance
  dispatcher: PartitionedDispatcher;
  backpressureMonitor?: BackpressureMonitor;
}

interface PullerMetrics {
  pullCount: number;
  eventsProcessed: number;
  gapsDetected: number;
  gapsRecovered: number;
  errors: number;
  lastPullLatencyMs: number;
  avgPullLatencyMs: number;
}

// Checkpoint counters for end-to-end delivery tracking
interface DeliveryCheckpoints {
  receivedFromProxy: number;   // CP1: Events received from HTTP response
  enqueuedToDispatcher: number; // CP2: Events dispatched to partitioned queues
}

// Buffer capacity constant (matches ws-proxy's event-log.ts)
const BUFFER_CAPACITY = 100_000;

export class EventPuller {
  private config: EventPullerConfig;
  private cursorManager: CursorManager;
  private httpClient: AxiosInstance;
  private running: boolean = false;
  private pullTimer: NodeJS.Timeout | null = null;

  // Metrics
  private metrics: PullerMetrics = {
    pullCount: 0,
    eventsProcessed: 0,
    gapsDetected: 0,
    gapsRecovered: 0,
    errors: 0,
    lastPullLatencyMs: 0,
    avgPullLatencyMs: 0
  };

  // End-to-end delivery checkpoints
  private checkpoints: DeliveryCheckpoints = {
    receivedFromProxy: 0,
    enqueuedToDispatcher: 0
  };

  constructor(config: Partial<EventPullerConfig> & {
    proxyUrl: string;
    redis: Redis;
    dispatcher: PartitionedDispatcher;
  }) {
    this.config = {
      pullIntervalMs: 50,
      batchSize: 1000,
      ...config
    };

    this.cursorManager = new CursorManager(config.redis);

    this.httpClient = axios.create({
      baseURL: config.proxyUrl,
      timeout: 5000,
      headers: {
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip'
      }
    });
  }

  /**
   * Start the pull loop
   */
  async start(): Promise<void> {
    if (this.running) {
      warnLog('EventPuller', 'Already running');
      return;
    }

    infoLog('EventPuller', 'Starting pull loop...');

    // Initialize cursor from Redis
    let cursor = await this.cursorManager.initialize();

    // If no cursor, sync to current head
    if (cursor === 0) {
      try {
        const headResponse = await this.httpClient.get<HeadResponse>('/api/head');
        await this.cursorManager.syncToHead(headResponse.data.head);
        cursor = headResponse.data.head;
        infoLog('EventPuller', `Synced to head: ${headResponse.data.head}`);
      } catch (error) {
        errorLog('EventPuller', `Failed to sync to head: ${error instanceof Error ? error.message : String(error)}`);
      }
    }

    // Log startup status with buffer utilization
    try {
      const proxyHead = await this.getProxyHead();
      if (proxyHead) {
        const lag = proxyHead.head - cursor;
        const bufferCount = proxyHead.count || 0;
        const utilizationPct = (bufferCount / BUFFER_CAPACITY) * 100;

        infoLog('EventPuller', `Startup | cursor=${cursor} | head=${proxyHead.head} | tail=${proxyHead.tail} | lag=${lag} | bufferUtil=${utilizationPct.toFixed(1)}%`);

        // Warn if we're starting with significant lag
        if (lag > BUFFER_CAPACITY * 0.5) {
          warnLog('EventPuller', `STARTUP_LAG_WARNING | lag=${lag} exceeds 50% buffer capacity | may miss events if buffer wraps`);
        }

        // Warn if buffer is heavily utilized
        if (utilizationPct > 70) {
          warnLog('EventPuller', `BUFFER_HIGH_UTILIZATION | ${utilizationPct.toFixed(1)}% | events may be overwritten soon`);
        }
      }
    } catch (error) {
      warnLog('EventPuller', `Could not get startup status: ${error instanceof Error ? error.message : String(error)}`);
    }

    this.running = true;
    this.schedulePull();

    infoLog('EventPuller', `Pull loop started (interval=${this.config.pullIntervalMs}ms, batch=${this.config.batchSize})`);
  }

  /**
   * Stop the pull loop
   */
  async stop(): Promise<void> {
    infoLog('EventPuller', 'Stopping pull loop...');

    this.running = false;

    if (this.pullTimer) {
      clearTimeout(this.pullTimer);
      this.pullTimer = null;
    }

    // Persist final cursor
    await this.cursorManager.persist();

    infoLog('EventPuller', 'Pull loop stopped');
  }

  /**
   * Schedule next pull with adaptive interval
   */
  private schedulePull(): void {
    if (!this.running) return;

    // Get adjusted interval based on backpressure
    let interval = this.config.pullIntervalMs;
    if (this.config.backpressureMonitor) {
      interval = this.config.backpressureMonitor.getAdjustedInterval(interval);
    }

    this.pullTimer = setTimeout(async () => {
      await this.pull();
      this.schedulePull();
    }, interval);
  }

  /**
   * Execute a single pull
   */
  private async pull(): Promise<void> {
    // Check backpressure state
    if (this.config.backpressureMonitor) {
      const state = this.config.backpressureMonitor.getState();
      if (state === 'paused') {
        debugLog('EventPuller', 'Skipping pull - paused due to backpressure');
        return;
      }
    }

    const startTime = Date.now();

    try {
      const cursor = this.cursorManager.getCursor();

      const response = await this.httpClient.get<PullResponse>('/api/events', {
        params: {
          after: cursor,
          limit: this.config.batchSize
        }
      });

      const { events, head, tail, count } = response.data;

      // CP1: Checkpoint - events received from proxy
      if (events.length > 0) {
        this.checkpoints.receivedFromProxy += events.length;
        debugLog('EventPuller', `CP_PROXY_RECV | count=${events.length} | cursor=${cursor} | head=${head} | total=${this.checkpoints.receivedFromProxy}`);
      }

      // Handle gap detection with detailed alerting
      if (cursor > 0 && this.cursorManager.hasGap(tail)) {
        const missedEvents = tail - cursor - 1;
        this.metrics.gapsDetected++;

        // Log as error for critical gaps
        errorLog('EventPuller', `GAP_DETECTED | cursor=${cursor} | tail=${tail} | missed=${missedEvents} events | resetting to tail`);

        await this.cursorManager.resetToTail(tail);
        this.metrics.gapsRecovered++;
        infoLog('EventPuller', `GAP_RECOVERED | newCursor=${tail} | gapsRecovered=${this.metrics.gapsRecovered}`);
        return;
      }

      // Check for sustained lag (>50% buffer capacity)
      const lag = head - cursor;
      const lagPct = (lag / BUFFER_CAPACITY) * 100;
      if (lagPct > 50 && this.metrics.pullCount % 20 === 0) {
        warnLog('EventPuller', `SUSTAINED_LAG | lag=${lag} (${lagPct.toFixed(1)}% of buffer) | head=${head} | cursor=${cursor}`);
      }

      // Process events
      if (events.length > 0) {
        await this.processEvents(events);

        // Advance cursor to last processed event
        const lastSeq = events[events.length - 1].seq;
        await this.cursorManager.advance(lastSeq);

        this.metrics.eventsProcessed += events.length;

        // Log progress periodically with buffer utilization
        if (this.metrics.pullCount % 100 === 0) {
          const currentLag = head - lastSeq;
          const bufferUtilPct = count ? (count / BUFFER_CAPACITY) * 100 : 0;
          debugLog('EventPuller', `Progress: pulled=${events.length}, cursor=${lastSeq}, head=${head}, lag=${currentLag}, bufferUtil=${bufferUtilPct.toFixed(1)}%`);
        }
      }

      this.metrics.pullCount++;

      // Update latency metrics
      const latency = Date.now() - startTime;
      this.metrics.lastPullLatencyMs = latency;
      this.metrics.avgPullLatencyMs =
        (this.metrics.avgPullLatencyMs * 0.95) + (latency * 0.05);

    } catch (error) {
      this.metrics.errors++;

      if (axios.isAxiosError(error)) {
        if (error.code === 'ECONNREFUSED') {
          errorLog('EventPuller', 'Connection refused - ws-proxy down?');
        } else if (error.code === 'ETIMEDOUT') {
          warnLog('EventPuller', 'Request timeout');
        } else {
          errorLog('EventPuller', `HTTP error: ${error.message}`);
        }
      } else {
        errorLog('EventPuller', `Pull error: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  }

  /**
   * Process batch of events
   */
  private async processEvents(events: NormalizedBidEvent[]): Promise<void> {
    for (const event of events) {
      // Dispatch to partitioned queues
      await this.config.dispatcher.dispatch(event);

      // CP2: Checkpoint - event enqueued to dispatcher
      this.checkpoints.enqueuedToDispatcher++;
    }

    // Log checkpoint progress periodically
    if (this.checkpoints.enqueuedToDispatcher % 1000 === 0) {
      debugLog('EventPuller', `CP_ENQUEUED | total=${this.checkpoints.enqueuedToDispatcher} | batch=${events.length}`);
    }
  }

  /**
   * Get current metrics including delivery checkpoints
   */
  getMetrics(): PullerMetrics & {
    cursor: number;
    cursorStats: ReturnType<CursorManager['getStats']>;
    checkpoints: DeliveryCheckpoints;
  } {
    return {
      ...this.metrics,
      cursor: this.cursorManager.getCursor(),
      cursorStats: this.cursorManager.getStats(),
      checkpoints: { ...this.checkpoints }
    };
  }

  /**
   * Check if running
   */
  isRunning(): boolean {
    return this.running;
  }

  /**
   * Get cursor manager for external use
   */
  getCursorManager(): CursorManager {
    return this.cursorManager;
  }

  /**
   * Force sync cursor to head (skip all current events)
   */
  async syncToHead(): Promise<void> {
    try {
      const headResponse = await this.httpClient.get<HeadResponse>('/api/head');
      await this.cursorManager.syncToHead(headResponse.data.head);
      infoLog('EventPuller', `Force synced to head: ${headResponse.data.head}`);
    } catch (error) {
      errorLog('EventPuller', `Failed to sync to head: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Get current head/tail from ws-proxy
   */
  async getProxyHead(): Promise<HeadResponse | null> {
    try {
      const response = await this.httpClient.get<HeadResponse>('/api/head');
      return response.data;
    } catch (error) {
      errorLog('EventPuller', `Failed to get head: ${error instanceof Error ? error.message : String(error)}`);
      return null;
    }
  }
}
