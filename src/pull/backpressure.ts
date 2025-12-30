/**
 * Backpressure Monitor for Cursor-Based Pull Architecture
 *
 * Monitors queue depth in the partitioned dispatcher and adjusts
 * the pull rate to prevent overwhelming the system.
 *
 * States:
 * - normal: Full speed pulling
 * - slowdown: Reduced pull rate (4x slower)
 * - paused: Stop pulling until queues drain
 */

import { PartitionedDispatcher } from './partitioned-dispatcher';
import { debugLog, warnLog, infoLog } from '../utils/debug-file-logger';

type BackpressureState = 'normal' | 'slowdown' | 'paused';

interface BackpressureConfig {
  maxQueuedEvents: number;       // Max events queued across all partitions
  maxPendingEvents: number;      // Max events being processed
  slowdownThreshold: number;     // Start slowing down at this utilization %
  pauseThreshold: number;        // Pause pulling at this utilization %
  checkIntervalMs: number;       // How often to check (default 1000ms)
}

export class BackpressureMonitor {
  private config: BackpressureConfig;
  private dispatcher: PartitionedDispatcher;
  private state: BackpressureState = 'normal';
  private checkTimer: NodeJS.Timeout | null = null;
  private lastLogTime: number = 0;
  private logIntervalMs: number = 10000; // Log state changes every 10s max

  constructor(
    dispatcher: PartitionedDispatcher,
    config?: Partial<BackpressureConfig>
  ) {
    this.dispatcher = dispatcher;
    this.config = {
      maxQueuedEvents: 10_000,
      maxPendingEvents: 1_000,
      slowdownThreshold: 0.7,
      pauseThreshold: 0.9,
      checkIntervalMs: 1000,
      ...config
    };
  }

  /**
   * Start monitoring
   */
  start(): void {
    if (this.checkTimer) return;

    this.checkTimer = setInterval(() => this.check(), this.config.checkIntervalMs);
    debugLog('Backpressure', 'Monitor started');
  }

  /**
   * Stop monitoring
   */
  stop(): void {
    if (this.checkTimer) {
      clearInterval(this.checkTimer);
      this.checkTimer = null;
    }
    debugLog('Backpressure', 'Monitor stopped');
  }

  /**
   * Check backpressure and update state
   */
  check(): void {
    const metrics = this.dispatcher.getMetrics();
    const totalLoad = metrics.totalQueued + metrics.totalPending;
    const maxLoad = this.config.maxQueuedEvents + this.config.maxPendingEvents;
    const utilization = totalLoad / maxLoad;

    const previousState = this.state;

    if (utilization >= this.config.pauseThreshold) {
      this.state = 'paused';
    } else if (utilization >= this.config.slowdownThreshold) {
      this.state = 'slowdown';
    } else if (utilization < this.config.slowdownThreshold * 0.8) {
      // Hysteresis: only return to normal when well below threshold
      this.state = 'normal';
    }

    // Log state changes (with rate limiting)
    if (previousState !== this.state) {
      const now = Date.now();
      if (now - this.lastLogTime > this.logIntervalMs) {
        const utilizationPct = (utilization * 100).toFixed(1);
        if (this.state === 'paused') {
          warnLog('Backpressure', `PAUSED: utilization=${utilizationPct}% (queued=${metrics.totalQueued}, pending=${metrics.totalPending})`);
        } else if (this.state === 'slowdown') {
          warnLog('Backpressure', `SLOWDOWN: utilization=${utilizationPct}% (queued=${metrics.totalQueued}, pending=${metrics.totalPending})`);
        } else {
          infoLog('Backpressure', `NORMAL: utilization=${utilizationPct}%`);
        }
        this.lastLogTime = now;
      }
    }
  }

  /**
   * Get current state
   */
  getState(): BackpressureState {
    return this.state;
  }

  /**
   * Get current utilization
   */
  getUtilization(): number {
    const metrics = this.dispatcher.getMetrics();
    const totalLoad = metrics.totalQueued + metrics.totalPending;
    const maxLoad = this.config.maxQueuedEvents + this.config.maxPendingEvents;
    return totalLoad / maxLoad;
  }

  /**
   * Get adjusted pull interval based on backpressure
   *
   * @param baseInterval - Base pull interval in ms
   * @returns Adjusted interval in ms
   */
  getAdjustedInterval(baseInterval: number): number {
    switch (this.state) {
      case 'paused':
        return 1000; // Check every 1s when paused
      case 'slowdown':
        return baseInterval * 4; // 4x slower
      default:
        return baseInterval;
    }
  }

  /**
   * Check if pulling should be paused
   */
  shouldPause(): boolean {
    return this.state === 'paused';
  }

  /**
   * Get metrics for monitoring
   */
  getMetrics(): {
    state: BackpressureState;
    utilization: number;
    config: BackpressureConfig;
  } {
    return {
      state: this.state,
      utilization: this.getUtilization(),
      config: this.config
    };
  }

  /**
   * Force state (for testing)
   */
  forceState(state: BackpressureState): void {
    this.state = state;
    warnLog('Backpressure', `State forced to: ${state}`);
  }

  /**
   * Update configuration
   */
  updateConfig(config: Partial<BackpressureConfig>): void {
    this.config = { ...this.config, ...config };
    debugLog('Backpressure', `Config updated: ${JSON.stringify(this.config)}`);
  }
}
