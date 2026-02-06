/**
 * Dynamic concurrency allocation between main and counterbid queues
 *
 * Strategy:
 * - Counterbid concurrency: FIXED at RATE_LIMIT × 1.5 (maximum priority)
 * - Main concurrency: DYNAMIC, inversely proportional to counterbid job count
 *   - 0 jobs → Main = RATE_LIMIT × 1.5
 *   - RATE_LIMIT jobs → Main = 1
 *   - Linear interpolation between these points
 * - Total concurrency: VARIABLE (ranges from RATE_LIMIT × 1.5 + 1 to RATE_LIMIT × 3)
 * - Real-time adjustment: Updates immediately on every job add/complete/fail event
 *
 * Example (RATE_LIMIT = 100):
 * - 0 CB jobs: CB=150, Main=150, Total=300
 * - 50 CB jobs: CB=150, Main=75, Total=225
 * - 100 CB jobs: CB=150, Main=1, Total=151
 */

import { Worker } from 'bullmq';
import { infoLog, warnLog, debugLog } from '../utils/debug-file-logger';

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

  constructor(totalConcurrency: number) {
    this.totalConcurrency = totalConcurrency;
    this.rateLimit = Number(process.env.RATE_LIMIT || 8);

    // Log initialization with formula
    infoLog(
      'ConcurrencyManager',
      `Initialized | totalConcurrency=${totalConcurrency} | rateLimit=${this.rateLimit} | Strategy: CB fixed at ${totalConcurrency}, Main dynamic 1-${totalConcurrency}`
    );
  }

  /**
   * Set worker array references for dynamic concurrency adjustment
   */
  setWorkers(mainWorkers: Worker[], counterbidWorkers: Worker[]) {
    this.mainWorkers = mainWorkers;
    this.counterbidWorkers = counterbidWorkers;
    infoLog('ConcurrencyManager', `Workers set - ready for dynamic allocation | main=${mainWorkers.length} | counterbid=${counterbidWorkers.length}`);
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
   * Adjust worker concurrency based on current counterbid workload
   *
   * New allocation formula (fixed counterbid priority):
   * - Counterbid concurrency: FIXED at maxConcurrency (RATE_LIMIT × 1.5)
   * - Main concurrency: Inversely proportional to counterbid job count
   *   - Linear interpolation: maxConcurrency → 1 as jobs go from 0 → RATE_LIMIT
   *   - Formula: 1 + (maxConcurrency - 1) × (1 - min(activeJobs, RATE_LIMIT) / RATE_LIMIT)
   *
   * Example (RATE_LIMIT=100, maxConcurrency=150):
   * - 0 jobs: CB=150, Main=150, Total=300
   * - 50 jobs: CB=150, Main=75, Total=225
   * - 100 jobs: CB=150, Main=1, Total=151
   */
  private adjustConcurrency() {
    const MAX_CONCURRENCY = this.totalConcurrency; // RATE_LIMIT × 1.5
    const MIN = this.minConcurrency;
    const active = this.activeCounterbidJobs;

    // Counterbid: FIXED at maximum concurrency (never changes)
    const cbConcurrency = MAX_CONCURRENCY;

    // Main: DYNAMIC, inversely proportional to counterbid job count
    // Clamp active jobs to RATE_LIMIT to prevent negative concurrency
    const clampedActive = Math.min(active, this.rateLimit);
    const ratio = clampedActive / this.rateLimit;
    const mainConcurrency = Math.max(MIN, Math.round(MAX_CONCURRENCY * (1 - ratio)));

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
    const percentLoad = ((ratio * 100).toFixed(1));

    // Detailed debug logging for verification
    infoLog(
      'ConcurrencyAlloc',
      `Active=${active}, CB=${actualCbTotal} (fixed, ${this.counterbidWorkers.length} workers), Main=${actualMainTotal} (dynamic, ${this.mainWorkers.length} workers), Total=${total} | Load=${percentLoad}% of RATE_LIMIT`
    );

    // Additional verification logs with formula details
    debugLog(
      'ConcurrencyAlloc',
      `Formula verification | RATE_LIMIT=${this.rateLimit} | maxConcurrency=${MAX_CONCURRENCY} | clampedActive=${clampedActive} | ratio=${ratio.toFixed(4)} | mainFormula=max(${MIN}, round(${MAX_CONCURRENCY} * (1 - ${ratio.toFixed(4)}))) = ${mainConcurrency} (distributed across ${this.mainWorkers.length} workers)`
    );
  }

  /**
   * Reconcile counter with actual queue depth (safety net for race conditions)
   * Only resets if drift is significant (>5 jobs)
   */
  async reconcile(actualJobCount: number) {
    const diff = Math.abs(actualJobCount - this.activeCounterbidJobs);

    // Log every reconciliation check for visibility
    infoLog('ConcurrencyReconcile', `Check | expected=${this.activeCounterbidJobs}, actual=${actualJobCount}, diff=${diff}`);

    if (diff > 5) {
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
export const concurrencyManager = new ConcurrencyManager(Number(process.env.RATE_LIMIT || 8) * 1.5);
