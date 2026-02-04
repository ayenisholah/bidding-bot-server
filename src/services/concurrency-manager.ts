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
  private mainWorker: Worker | null = null;
  private counterbidWorker: Worker | null = null;
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
   * Set worker references for dynamic concurrency adjustment
   */
  setWorkers(main: Worker, counterbid: Worker) {
    this.mainWorker = main;
    this.counterbidWorker = counterbid;
    infoLog('ConcurrencyManager', 'Workers set - ready for dynamic allocation');
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

    // Apply to workers (BullMQ allows dynamic concurrency adjustment)
    if (this.counterbidWorker) {
      this.counterbidWorker.concurrency = cbConcurrency;
    }
    if (this.mainWorker) {
      this.mainWorker.concurrency = mainConcurrency;
    }

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

    // Enhanced logging with total concurrency and ratio
    const total = cbConcurrency + mainConcurrency;
    const percentLoad = ((ratio * 100).toFixed(1));

    // Detailed debug logging for verification
    infoLog(
      'ConcurrencyAlloc',
      `Active=${active}, CB=${cbConcurrency} (fixed), Main=${mainConcurrency} (dynamic), Total=${total} | Load=${percentLoad}% of RATE_LIMIT`
    );

    // Additional verification logs with formula details
    debugLog(
      'ConcurrencyAlloc',
      `Formula verification | RATE_LIMIT=${this.rateLimit} | maxConcurrency=${MAX_CONCURRENCY} | clampedActive=${clampedActive} | ratio=${ratio.toFixed(4)} | mainFormula=max(${MIN}, round(${MAX_CONCURRENCY} * (1 - ${ratio.toFixed(4)}))) = ${mainConcurrency}`
    );

    // Log when concurrency changes (only log actual changes to reduce noise)
    const prevCbConcurrency = this.counterbidWorker?.concurrency ?? 0;
    const prevMainConcurrency = this.mainWorker?.concurrency ?? 0;

    if (prevCbConcurrency !== cbConcurrency || prevMainConcurrency !== mainConcurrency) {
      infoLog(
        'ConcurrencyAlloc',
        `CHANGE DETECTED | CB: ${prevCbConcurrency}→${cbConcurrency} | Main: ${prevMainConcurrency}→${mainConcurrency} | Total: ${prevCbConcurrency + prevMainConcurrency}→${total}`
      );
    }
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
    return {
      activeCounterbidJobs: this.activeCounterbidJobs,
      totalConcurrency: this.totalConcurrency,
      rateLimit: this.rateLimit,
      currentAllocation: {
        counterbid: this.counterbidWorker?.concurrency ?? 0,
        main: this.mainWorker?.concurrency ?? 0,
        total: (this.counterbidWorker?.concurrency ?? 0) + (this.mainWorker?.concurrency ?? 0),
      },
      lastAdjustment: this.lastAdjustment,
      adjustmentHistory: this.adjustmentHistory,
    };
  }
}

// Singleton export with RATE_LIMIT from environment
export const concurrencyManager = new ConcurrencyManager(Number(process.env.RATE_LIMIT || 8) * 1.5);
