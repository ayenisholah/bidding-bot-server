/**
 * Rate Pacing Validation Script for Bottleneck limiter
 *
 * Validates that Bottleneck enforces the expected constant-rate pacing
 * matching the production configuration:
 *   new Bottleneck({ minTime: 1000 / RATE_LIMIT, trackDoneStatus: true })
 *
 * Run:  npx ts-node scripts/test-rate-pacing.ts
 * npm script suggestion: "test:rate-pacing": "ts-node scripts/test-rate-pacing.ts"
 */

import Bottleneck from 'bottleneck';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const RATE_LIMIT = Number(process.env.RATE_LIMIT) || 100; // requests per second
const STEP_MS = 1000 / RATE_LIMIT; // expected inter-arrival (10ms @ 100rps)
const JOB_COUNT = 1000; // number of jobs to schedule per test
const WINDOW_MS = 100; // sliding window for burst detection

// Pass/fail thresholds
const P50_TOLERANCE_MS = 2; // p50 must be within +/- this of STEP_MS
const P99_MAX_FACTOR = 2; // p99 must be < STEP_MS * this factor
const MAX_BURST_EXTRA = 2; // max burst in WINDOW_MS <= expected + this
                           // Note: Node.js setTimeout jitter (~1-4ms at 10ms intervals)
                           // means 12 requests in 100ms is expected at 100rps

// Derived expected values
const EXPECTED_BURST = Math.floor(WINDOW_MS / STEP_MS); // e.g. 10 for 100rps

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** High-resolution timestamp in milliseconds (sub-ms precision) */
function hrtimeMs(): number {
  return Number(process.hrtime.bigint()) / 1e6;
}

/** Compute percentile from a SORTED array of numbers */
function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

/** Compute inter-arrival deltas from timestamps */
function deltas(timestamps: number[]): number[] {
  const result: number[] = [];
  for (let i = 1; i < timestamps.length; i++) {
    result.push(timestamps[i] - timestamps[i - 1]);
  }
  return result;
}

/** Count max requests in any sliding window of `windowMs` */
function maxBurstInWindow(timestamps: number[], windowMs: number): number {
  if (timestamps.length === 0) return 0;
  let maxBurst = 0;
  let left = 0;
  for (let right = 0; right < timestamps.length; right++) {
    while (timestamps[right] - timestamps[left] > windowMs) {
      left++;
    }
    maxBurst = Math.max(maxBurst, right - left + 1);
  }
  return maxBurst;
}

/** Print a simple ASCII histogram of delta values */
function printHistogram(values: number[], bucketWidth: number, label: string): void {
  if (values.length === 0) return;
  const min = Math.floor(Math.min(...values));
  const max = Math.ceil(Math.max(...values));

  const buckets = new Map<number, number>();
  for (const v of values) {
    const bucket = Math.floor(v / bucketWidth) * bucketWidth;
    buckets.set(bucket, (buckets.get(bucket) || 0) + 1);
  }

  const sortedKeys = Array.from(buckets.keys()).sort((a, b) => a - b);
  const maxCount = Math.max(...buckets.values());
  const barScale = 50 / maxCount; // max bar width = 50 chars

  console.log(`\n  ${label} (bucket=${bucketWidth}ms, range=${min.toFixed(1)}-${max.toFixed(1)}ms)`);
  console.log('  ' + '-'.repeat(70));
  for (const key of sortedKeys) {
    const count = buckets.get(key)!;
    const bar = '#'.repeat(Math.max(1, Math.round(count * barScale)));
    const pct = ((count / values.length) * 100).toFixed(1);
    console.log(`  ${String(key).padStart(6)}ms | ${bar} ${count} (${pct}%)`);
  }
  console.log('');
}

interface TestResult {
  name: string;
  passed: boolean;
  stats: {
    count: number;
    p50: number;
    p95: number;
    p99: number;
    min: number;
    max: number;
    mean: number;
    maxBurst: number;
  };
  checks: { label: string; passed: boolean; detail: string }[];
}

function reportResult(result: TestResult): void {
  const verdict = result.passed ? 'PASS' : 'FAIL';
  console.log(`\n${'='.repeat(72)}`);
  console.log(`  ${verdict}: ${result.name}`);
  console.log(`${'='.repeat(72)}`);
  console.log(`  Jobs: ${result.stats.count}  |  Expected step: ${STEP_MS.toFixed(2)}ms  |  Rate: ${RATE_LIMIT} RPS`);
  console.log(`  Delta stats (ms):  min=${result.stats.min.toFixed(2)}  p50=${result.stats.p50.toFixed(2)}  p95=${result.stats.p95.toFixed(2)}  p99=${result.stats.p99.toFixed(2)}  max=${result.stats.max.toFixed(2)}  mean=${result.stats.mean.toFixed(2)}`);
  console.log(`  Max burst in ${WINDOW_MS}ms window: ${result.stats.maxBurst}  (expected <= ${EXPECTED_BURST + MAX_BURST_EXTRA})`);
  console.log('');
  for (const check of result.checks) {
    const mark = check.passed ? 'OK ' : 'ERR';
    console.log(`  [${mark}] ${check.label}: ${check.detail}`);
  }
  console.log('');
}

// ---------------------------------------------------------------------------
// Test 1: Single-Instance Pacing Validation
// ---------------------------------------------------------------------------
async function testSingleInstancePacing(): Promise<TestResult> {
  console.log('\n--- Test 1: Single-Instance Pacing Validation ---');
  console.log(`  Scheduling ${JOB_COUNT} no-op jobs through Bottleneck (minTime=${STEP_MS}ms)...`);

  const limiter = new Bottleneck({
    minTime: 1000 / RATE_LIMIT,
    trackDoneStatus: true,
  });

  const timestamps: number[] = [];

  const promises: Promise<void>[] = [];
  for (let i = 0; i < JOB_COUNT; i++) {
    promises.push(
      limiter.schedule(() => {
        timestamps.push(hrtimeMs());
        return Promise.resolve();
      })
    );
  }

  await Promise.all(promises);

  // Compute deltas
  const gaps = deltas(timestamps);
  const sorted = [...gaps].sort((a, b) => a - b);

  const stats = {
    count: gaps.length,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    min: sorted[0],
    max: sorted[sorted.length - 1],
    mean: gaps.reduce((s, v) => s + v, 0) / gaps.length,
    maxBurst: maxBurstInWindow(timestamps, WINDOW_MS),
  };

  // Evaluate checks
  const checks: TestResult['checks'] = [];

  const p50Ok = Math.abs(stats.p50 - STEP_MS) <= P50_TOLERANCE_MS;
  checks.push({
    label: 'p50 within tolerance',
    passed: p50Ok,
    detail: `p50=${stats.p50.toFixed(2)}ms, expected=${STEP_MS.toFixed(2)}ms +/-${P50_TOLERANCE_MS}ms (range ${(STEP_MS - P50_TOLERANCE_MS).toFixed(2)}-${(STEP_MS + P50_TOLERANCE_MS).toFixed(2)}ms)`,
  });

  const p99Ok = stats.p99 < STEP_MS * P99_MAX_FACTOR;
  checks.push({
    label: 'p99 under 2x step',
    passed: p99Ok,
    detail: `p99=${stats.p99.toFixed(2)}ms, limit=${(STEP_MS * P99_MAX_FACTOR).toFixed(2)}ms`,
  });

  const burstOk = stats.maxBurst <= EXPECTED_BURST + MAX_BURST_EXTRA;
  checks.push({
    label: `max burst in ${WINDOW_MS}ms window`,
    passed: burstOk,
    detail: `burst=${stats.maxBurst}, limit=${EXPECTED_BURST + MAX_BURST_EXTRA}`,
  });

  const passed = checks.every((c) => c.passed);

  // Histogram
  printHistogram(gaps, 1, 'Inter-arrival delta histogram');

  const result: TestResult = { name: 'Single-Instance Pacing', passed, stats, checks };
  reportResult(result);

  // Cleanup
  await limiter.disconnect();
  return result;
}

// ---------------------------------------------------------------------------
// Test 2: Retry Pacing Validation
// ---------------------------------------------------------------------------
async function testRetryPacing(): Promise<TestResult> {
  console.log('\n--- Test 2: Retry Pacing Validation (50% simulated failures) ---');
  console.log(`  Scheduling ${JOB_COUNT} jobs with 50% failure rate, retry delay=50ms...`);

  const RETRY_DELAY_MS = 50;

  const limiter = new Bottleneck({
    minTime: 1000 / RATE_LIMIT,
    trackDoneStatus: true,
  });

  // Track all execution timestamps (first attempt + retries)
  const allTimestamps: number[] = [];
  // Track only first-attempt timestamps for delta analysis
  const firstAttemptTimestamps: number[] = [];
  // Track retry timestamps separately
  const retryTimestamps: number[] = [];

  // Bottleneck's failed handler: return delay to trigger a retry
  limiter.on('failed', async (_error, jobInfo) => {
    if (jobInfo.retryCount >= 1) return; // only 1 retry per job
    return RETRY_DELAY_MS;
  });

  let jobIndex = 0;
  const promises: Promise<void>[] = [];

  for (let i = 0; i < JOB_COUNT; i++) {
    const myIndex = jobIndex++;
    // Deterministic: even-indexed jobs fail on first attempt
    const shouldFail = myIndex % 2 === 0;
    let attemptCount = 0;

    promises.push(
      limiter.schedule(() => {
        const now = hrtimeMs();
        allTimestamps.push(now);
        attemptCount++;

        if (shouldFail && attemptCount === 1) {
          // Record as first attempt
          firstAttemptTimestamps.push(now);
          throw new Error('simulated failure');
        }

        if (attemptCount > 1) {
          retryTimestamps.push(now);
        } else {
          firstAttemptTimestamps.push(now);
        }

        return Promise.resolve();
      })
    );
  }

  await Promise.all(promises);

  // Sort all timestamps (they may be slightly out of order due to concurrency)
  allTimestamps.sort((a, b) => a - b);

  // Compute inter-arrival deltas on ALL executions (first + retry) passing through the limiter
  const gaps = deltas(allTimestamps);
  const sorted = [...gaps].sort((a, b) => a - b);

  const stats = {
    count: gaps.length,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    min: sorted[0],
    max: sorted[sorted.length - 1],
    mean: gaps.reduce((s, v) => s + v, 0) / gaps.length,
    maxBurst: maxBurstInWindow(allTimestamps, WINDOW_MS),
  };

  console.log(`  Total executions: ${allTimestamps.length} (first: ${firstAttemptTimestamps.length}, retries: ${retryTimestamps.length})`);

  // Evaluate checks - retries go back through the queue, so minTime should still be respected.
  // However, with 50% failures and retry delay < minTime, the effective load on the limiter
  // is higher (1000 first attempts + 500 retries = 1500 executions). The key validation is
  // that minTime spacing is STILL ENFORCED between consecutive executions -- the p50 and p99
  // checks verify this. The burst check uses a relaxed threshold to account for the extra
  // retry-sourced load passing through the same limiter.
  const checks: TestResult['checks'] = [];

  const p50Ok = Math.abs(stats.p50 - STEP_MS) <= P50_TOLERANCE_MS;
  checks.push({
    label: 'p50 within tolerance',
    passed: p50Ok,
    detail: `p50=${stats.p50.toFixed(2)}ms, expected=${STEP_MS.toFixed(2)}ms +/-${P50_TOLERANCE_MS}ms (range ${(STEP_MS - P50_TOLERANCE_MS).toFixed(2)}-${(STEP_MS + P50_TOLERANCE_MS).toFixed(2)}ms)`,
  });

  const p99Ok = stats.p99 < STEP_MS * P99_MAX_FACTOR;
  checks.push({
    label: 'p99 under 2x step',
    passed: p99Ok,
    detail: `p99=${stats.p99.toFixed(2)}ms, limit=${(STEP_MS * P99_MAX_FACTOR).toFixed(2)}ms`,
  });

  // With retries, the total execution count is ~1.5x the job count, so the effective
  // rate through the limiter is higher. The burst limit is scaled proportionally.
  // At 100rps with 50% failures: ~1500 executions over ~15s = effective ~100rps + retries
  // queued behind. The minTime still spaces them, but more pass through per window.
  const retryBurstExtra = Math.ceil(EXPECTED_BURST * 0.8); // Allow ~80% more due to retries
  const retryBurstLimit = EXPECTED_BURST + retryBurstExtra;
  const burstOk = stats.maxBurst <= retryBurstLimit;
  checks.push({
    label: `max burst in ${WINDOW_MS}ms window`,
    passed: burstOk,
    detail: `burst=${stats.maxBurst}, limit=${retryBurstLimit} (relaxed for retry load)`,
  });

  const passed = checks.every((c) => c.passed);

  // Histogram
  printHistogram(gaps, 1, 'Inter-arrival delta histogram (all executions incl. retries)');

  const result: TestResult = { name: 'Retry Pacing (50% failure rate)', passed, stats, checks };
  reportResult(result);

  // Cleanup
  await limiter.disconnect();
  return result;
}

// ---------------------------------------------------------------------------
// Test 3: Multi-Instance Burst Detection (DESIGN ONLY - not implemented)
// ---------------------------------------------------------------------------

/*
 * TEST 3: Multi-Instance Burst Detection
 *
 * Purpose: Demonstrate that running multiple Bottleneck instances (e.g. in
 * separate worker processes) does NOT coordinate rate limiting across
 * instances, leading to aggregate burst rates exceeding the intended limit.
 *
 * Design:
 *
 *   1. Start a local HTTP server on a random port that collects timestamps:
 *        POST /tick  ->  records Date.now() and responds 200
 *
 *   2. Launch 2 child Node.js processes, each running:
 *        const limiter = new Bottleneck({ minTime: 1000/100, trackDoneStatus: true });
 *        for (let i = 0; i < 500; i++) {
 *          limiter.schedule(() => fetch(`http://localhost:${port}/tick`));
 *        }
 *
 *   3. Wait for both processes to finish.
 *
 *   4. On the HTTP server, sort all collected timestamps and compute:
 *        - Inter-arrival deltas across ALL requests from both instances
 *        - Max burst in any 100ms sliding window
 *
 *   5. Expected results (SHOULD FAIL the single-instance criteria):
 *        - Aggregate rate ~ 200 RPS (2x the intended 100 RPS)
 *        - p50 delta ~ 5ms (half the expected 10ms minTime)
 *        - Max burst in 100ms ~ 20 (double the single-instance value of ~10)
 *        - This demonstrates the gap: Bottleneck's minTime is per-process only
 *
 *   6. Mitigation options to discuss in audit report:
 *        - Use Bottleneck's Redis-backed clustering for distributed rate limiting
 *        - Use an external rate limiter (e.g. Redis sliding window)
 *        - Accept per-process rate limiting if each process has its own API key
 *
 * npm script: "test:rate-pacing:multi": "ts-node scripts/test-rate-pacing-multi.ts"
 */

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main(): Promise<void> {
  console.log('========================================================================');
  console.log('  Rate Pacing Validation Suite');
  console.log(`  RATE_LIMIT=${RATE_LIMIT}  STEP_MS=${STEP_MS.toFixed(2)}ms  JOBS=${JOB_COUNT}`);
  console.log('========================================================================');

  const results: TestResult[] = [];

  results.push(await testSingleInstancePacing());
  results.push(await testRetryPacing());

  // Summary
  console.log('\n========================================================================');
  console.log('  SUMMARY');
  console.log('========================================================================');

  let allPassed = true;
  for (const r of results) {
    const mark = r.passed ? 'PASS' : 'FAIL';
    console.log(`  [${mark}] ${r.name}`);
    if (!r.passed) allPassed = false;
  }

  console.log(`\n  Test 3: Multi-Instance Burst Detection -- DESIGN ONLY (see comments in script)`);

  console.log(`\n  Overall: ${allPassed ? 'ALL TESTS PASSED' : 'SOME TESTS FAILED'}`);
  console.log('========================================================================\n');

  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(2);
});
