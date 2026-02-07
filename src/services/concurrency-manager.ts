/**
 * Concurrency allocation between main and counterbid queues
 *
 * Strategy:
 * - Counterbid concurrency: FIXED at RATE_LIMIT
 * - Main concurrency: FIXED at RATE_LIMIT (no longer reduced by counterbid activity)
 * - Total concurrency: FIXED at RATE_LIMIT × 2
 *
 * BullMQ concurrency only controls parallelism (how many jobs run concurrently).
 * The Bottleneck rate limiter enforces the actual API rate (RPS).
 * Reducing BullMQ concurrency below RATE_LIMIT starves the Bottleneck queue,
 * causing RPS to drop well below the configured rate.
 */

import { Worker } from 'bullmq';
import { warnLog } from '../utils/debug-file-logger';

class ConcurrencyManager {
  private activeCounterbidJobs = 0;
  private totalConcurrency: number;
  private rateLimit: number;
  private minConcurrency = 1;
  private mainWorkers: Worker[] = [];
  private counterbidWorkers: Worker[] = [];
  private lastAdjustment: string = '';
  private adjustmentHistory: Array<{
    timestamp: string;
    activeJobs: number;
    counterbidConcurrency: number;
    mainConcurrency: number;
  }> = [];
  private lastLoggedActive = 0; // Track last logged value for threshold-based logging

  constructor(totalConcurrency: number) {
    this.totalConcurrency = totalConcurrency;
    this.rateLimit = Number(process.env.RATE_LIMIT || 8);
    this.minConcurrency = this.rateLimit;

    // Log initialization with formula
  }

  /**
   * Set worker array references for dynamic concurrency adjustment
   */
  setWorkers(mainWorkers: Worker[], counterbidWorkers: Worker[]) {
    this.mainWorkers = mainWorkers;
    this.counterbidWorkers = counterbidWorkers;
  }

  /**
   * Increment counterbid job counter (called when job added to counterbid queue)
   * Triggers immediate concurrency reallocation
   */
  incrementCounterbid() {
    this.activeCounterbidJobs++;
    this.adjustConcurrency();
  }

  /**
   * Decrement counterbid job counter (called when job completed/failed)
   * Triggers immediate concurrency reallocation
   */
  decrementCounterbid() {
    this.activeCounterbidJobs = Math.max(0, this.activeCounterbidJobs - 1);
    this.adjustConcurrency();
  }

  /**
   * Decrement counterbid jobs by a batch count (used when queued jobs are removed directly).
   */
  decrementCounterbidBy(count: number) {
    if (!Number.isFinite(count) || count <= 0) return;
    this.activeCounterbidJobs = Math.max(0, this.activeCounterbidJobs - Math.floor(count));
    this.adjustConcurrency();
  }

  /**
   * Adjust worker concurrency based on current counterbid workload
   *
   * Main concurrency is FIXED at RATE_LIMIT — it is never reduced by counterbid
   * activity. The Bottleneck rate limiter enforces the actual API rate; BullMQ
   * concurrency only controls how many jobs can feed the limiter concurrently.
   */
  private adjustConcurrency() {
    const MAX_CONCURRENCY = this.totalConcurrency; // RATE_LIMIT
    const MIN_MAIN = this.rateLimit;
    const active = this.activeCounterbidJobs;

    // Counterbid: FIXED at maximum concurrency (never changes)
    const cbConcurrency = MAX_CONCURRENCY;

    // Main: FIXED at RATE_LIMIT (MIN_MAIN == rateLimit, so max() always returns rateLimit)
    const mainConcurrency = Math.max(MIN_MAIN, this.rateLimit - active);

    // Distribute concurrency evenly across workers in each array
    // Split counterbid concurrency evenly across counterbid workers
    const cbPerWorker = Math.floor(cbConcurrency / this.counterbidWorkers.length);
    const cbRemainder = cbConcurrency % this.counterbidWorkers.length;

    this.counterbidWorkers.forEach((worker, index) => {
      // Distribute remainder to first workers
      worker.concurrency = cbPerWorker + (index < cbRemainder ? 1 : 0);
    });

    // Split main concurrency evenly across main workers
    const mainPerWorker = Math.floor(mainConcurrency / this.mainWorkers.length);
    const mainRemainder = mainConcurrency % this.mainWorkers.length;

    this.mainWorkers.forEach((worker, index) => {
      worker.concurrency = mainPerWorker + (index < mainRemainder ? 1 : 0);
    });

    // Record adjustment
    const timestamp = new Date().toISOString();
    this.lastAdjustment = timestamp;
    this.adjustmentHistory.push({
      timestamp,
      activeJobs: active,
      counterbidConcurrency: cbConcurrency,
      mainConcurrency: mainConcurrency,
    });

    // Keep only last 100 adjustments
    if (this.adjustmentHistory.length > 100) {
      this.adjustmentHistory.shift();
    }

    // Calculate actual totals after worker distribution
    const actualCbTotal = this.counterbidWorkers.reduce((sum, w) => sum + w.concurrency, 0);
    const actualMainTotal = this.mainWorkers.reduce((sum, w) => sum + w.concurrency, 0);
    const total = actualCbTotal + actualMainTotal;
    const percentLoad = ((active / this.rateLimit) * 100).toFixed(1);

    // Only log if active jobs changed by >10 (reduces log volume from 175K → ~5K entries)
    if (Math.abs(active - this.lastLoggedActive) >= 10) {
      this.lastLoggedActive = active;
    }
  }

  /**
   * Reconcile counter with actual queue depth (safety net for race conditions).
   */
  async reconcile(actualJobCount: number) {
    const diff = Math.abs(actualJobCount - this.activeCounterbidJobs);

    // Log every reconciliation check for visibility

    if (diff > 0) {
      warnLog(
        'ConcurrencyAlloc',
        `Counter drift detected: expected=${this.activeCounterbidJobs}, actual=${actualJobCount}, diff=${diff}, resetting to actual`
      );
      this.activeCounterbidJobs = actualJobCount;
      this.adjustConcurrency();
    }
  }

  /**
   * Get current allocation statistics
   */
  getStats() {
    const mainConcurrency = this.mainWorkers.reduce((sum, w) => sum + w.concurrency, 0);
    const counterbidConcurrency = this.counterbidWorkers.reduce((sum, w) => sum + w.concurrency, 0);

    return {
      activeCounterbidJobs: this.activeCounterbidJobs,
      totalConcurrency: this.totalConcurrency,
      rateLimit: this.rateLimit,
      currentAllocation: {
        counterbid: counterbidConcurrency,
        main: mainConcurrency,
        total: mainConcurrency + counterbidConcurrency,
      },
      workers: {
        main: this.mainWorkers.map((w, i) => ({ index: i, concurrency: w.concurrency })),
        counterbid: this.counterbidWorkers.map((w, i) => ({ index: i, concurrency: w.concurrency })),
      },
      lastAdjustment: this.lastAdjustment,
      adjustmentHistory: this.adjustmentHistory,
    };
  }
}

// Singleton export with RATE_LIMIT from environment
export const concurrencyManager = new ConcurrencyManager(Number(process.env.RATE_LIMIT || 8));
