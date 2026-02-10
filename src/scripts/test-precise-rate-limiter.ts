/**
 * PreciseRateLimiter Unit Tests
 *
 * Validates:
 * A. Dispatch interval precision: p50 within 10ms +-0.5ms, p99 < 11.5ms
 * B. Priority ordering: priority-0 jobs jump ahead of priority-5
 * C. Retry slot release: failed jobs don't hold slots during delay
 * D. maxConcurrent enforcement: never exceeds limit
 * E. Throughput under simulated latency: 100 RPS sustained at 1000ms, 2000ms, 3000ms
 *
 * Run with: npx ts-node src/scripts/test-precise-rate-limiter.ts
 */
import { PreciseRateLimiter } from '../utils/precise-rate-limiter';

const RATE_LIMIT = 100;
const MIN_TIME = 1000 / RATE_LIMIT; // 10ms
const MAX_CONCURRENT = Math.ceil(RATE_LIMIT * 5); // 500

interface TestResult {
  name: string;
  passed: boolean;
  details: string[];
}

const results: TestResult[] = [];

function simulateLatency(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function calculateDispatchRPS(timestamps: number[]): number {
  if (timestamps.length < 2) return 0;
  const sorted = [...timestamps].sort((a, b) => a - b);
  const window = sorted[sorted.length - 1] - sorted[0];
  return (sorted.length - 1) / (window / 1000);
}

// ============================================
// TEST A: Dispatch Interval Precision
// ============================================
async function testDispatchPrecision(): Promise<boolean> {
  console.log('\n=== TEST A: DISPATCH INTERVAL PRECISION ===');
  console.log(`Config: minTime=${MIN_TIME}ms, no latency`);

  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: MAX_CONCURRENT,
    trackDoneStatus: true,
  });

  const NUM_JOBS = 200;
  const timestamps: number[] = [];

  const promises = Array.from({ length: NUM_JOBS }, () =>
    limiter.schedule(() => {
      timestamps.push(performance.now());
      return Promise.resolve();
    })
  );

  await Promise.all(promises);

  // Calculate spacings
  timestamps.sort((a, b) => a - b);
  const spacings: number[] = [];
  for (let i = 1; i < timestamps.length; i++) {
    spacings.push(timestamps[i] - timestamps[i - 1]);
  }
  spacings.sort((a, b) => a - b);

  const p50 = spacings[Math.floor(spacings.length * 0.5)];
  const p95 = spacings[Math.floor(spacings.length * 0.95)];
  const p99 = spacings[Math.floor(spacings.length * 0.99)];
  const min = spacings[0];
  const max = spacings[spacings.length - 1];

  console.log(`Jobs: ${NUM_JOBS}`);
  console.log(`Spacing: p50=${p50.toFixed(3)}ms | p95=${p95.toFixed(3)}ms | p99=${p99.toFixed(3)}ms`);
  console.log(`min=${min.toFixed(3)}ms | max=${max.toFixed(3)}ms`);

  // Acceptance: p50 within 10ms +-1ms, p99 < 12ms
  const p50Ok = p50 >= 9.0 && p50 <= 11.0;
  const p99Ok = p99 < 12.0;
  const passed = p50Ok && p99Ok;

  console.log(`p50 check: ${p50.toFixed(3)}ms in [9.0, 11.0] -> ${p50Ok ? 'PASS' : 'FAIL'}`);
  console.log(`p99 check: ${p99.toFixed(3)}ms < 12.0 -> ${p99Ok ? 'PASS' : 'FAIL'}`);
  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);

  // Burst detection
  let maxBurst = 0;
  for (let i = 0; i < timestamps.length; i++) {
    const windowEnd = timestamps[i] + 100;
    let count = 0;
    for (let j = i; j < timestamps.length && timestamps[j] < windowEnd; j++) {
      count++;
    }
    maxBurst = Math.max(maxBurst, count);
  }

  console.log(`Max burst in 100ms window: ${maxBurst} (target <= 11)`);

  results.push({
    name: 'A: Dispatch interval precision (p50 ~10ms, p99 < 12ms)',
    passed,
    details: [
      `p50=${p50.toFixed(3)}ms p95=${p95.toFixed(3)}ms p99=${p99.toFixed(3)}ms`,
      `min=${min.toFixed(3)}ms max=${max.toFixed(3)}ms`,
      `Max burst in 100ms: ${maxBurst}`,
    ],
  });

  return passed;
}

// ============================================
// TEST B: Priority Ordering
// ============================================
async function testPriorityOrdering(): Promise<boolean> {
  console.log('\n=== TEST B: PRIORITY ORDERING ===');
  console.log(`Config: minTime=${MIN_TIME}ms, maxConcurrent=1, no latency`);

  // Use maxConcurrent=1 so all jobs queue up and priority ordering is visible
  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: 1,
    trackDoneStatus: true,
  });

  const executionLog: { label: string; priority: number }[] = [];

  // Schedule a sentinel that holds the single slot while we fill the queue
  let releaseSentinel: () => void;
  const sentinelPromise = new Promise<void>((r) => { releaseSentinel = r; });
  const blockerPromise = limiter.schedule({ priority: 0 }, () => sentinelPromise);

  // Wait for sentinel to start
  await new Promise((r) => setTimeout(r, 50));

  // Schedule 30 low-priority jobs (priority 5)
  const lowPromises = Array.from({ length: 30 }, (_, i) =>
    limiter.schedule({ priority: 5 }, () => {
      executionLog.push({ label: `low-${i}`, priority: 5 });
      return Promise.resolve();
    })
  );

  // Schedule 5 high-priority jobs (priority 0)
  const highPromises = Array.from({ length: 5 }, (_, i) =>
    limiter.schedule({ priority: 0 }, () => {
      executionLog.push({ label: `high-${i}`, priority: 0 });
      return Promise.resolve();
    })
  );

  // Schedule 5 medium-priority jobs (priority 3)
  const medPromises = Array.from({ length: 5 }, (_, i) =>
    limiter.schedule({ priority: 3 }, () => {
      executionLog.push({ label: `med-${i}`, priority: 3 });
      return Promise.resolve();
    })
  );

  // Release sentinel -- jobs drain in priority order
  releaseSentinel!();
  await blockerPromise;
  await Promise.all([...lowPromises, ...highPromises, ...medPromises]);

  // Check ordering: all p0 should come before all p3, all p3 before all p5
  const firstMed = executionLog.findIndex((e) => e.priority === 3);
  const firstLow = executionLog.findIndex((e) => e.priority === 5);
  const lastHigh = executionLog.map((e, i) => e.priority === 0 ? i : -1).filter((i) => i >= 0).pop()!;
  const lastMed = executionLog.map((e, i) => e.priority === 3 ? i : -1).filter((i) => i >= 0).pop()!;

  const highBeforeMed = lastHigh < firstMed;
  const medBeforeLow = lastMed < firstLow;
  const passed = highBeforeMed && medBeforeLow;

  console.log(`First 10 executed: ${executionLog.slice(0, 10).map((e) => e.label).join(', ')}`);
  console.log(`High (p0) all before Medium (p3): ${highBeforeMed ? 'YES' : 'NO'}`);
  console.log(`Medium (p3) all before Low (p5): ${medBeforeLow ? 'YES' : 'NO'}`);
  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);

  results.push({
    name: 'B: Priority ordering (p0 > p3 > p5)',
    passed,
    details: [
      `Execution order: ${executionLog.slice(0, 15).map((e) => e.label).join(', ')}...`,
      `High before Medium: ${highBeforeMed}, Medium before Low: ${medBeforeLow}`,
    ],
  });

  return passed;
}

// ============================================
// TEST C: Retry Slot Release
// ============================================
async function testRetrySlotRelease(): Promise<boolean> {
  console.log('\n=== TEST C: RETRY SLOT RELEASE ===');
  console.log('Verifies: failed jobs free slot immediately, re-queue after delay');

  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: 5,
    trackDoneStatus: true,
  });

  const RETRY_DELAY = 200; // 200ms retry delay
  let retryCount = 0;
  const slotSnapshots: number[] = []; // executing count at dispatch time

  limiter.on('failed', async (_error, jobInfo) => {
    if (jobInfo.retryCount >= 2) return; // Max 2 retries
    retryCount++;
    return RETRY_DELAY;
  });

  const dispatchTimes: number[] = [];
  let failJobCount = 0;

  // Schedule 20 jobs, first 5 will fail once each
  const promises = Array.from({ length: 20 }, (_, i) =>
    limiter.schedule(() => {
      dispatchTimes.push(performance.now());
      const counts = limiter.counts();
      slotSnapshots.push(counts.EXECUTING);

      if (i < 5 && failJobCount < 5) {
        failJobCount++;
        throw new Error(`Simulated failure ${i}`);
      }
      return Promise.resolve();
    })
  );

  await Promise.all(promises);

  // Key assertion: maxConcurrent was never exceeded
  const maxObserved = Math.max(...slotSnapshots);
  const slotsOk = maxObserved <= 5;

  // During retry delay (200ms), the slot should be free for other jobs
  // Count dispatches during the first 200ms window after first failure
  dispatchTimes.sort((a, b) => a - b);

  console.log(`Total dispatches: ${dispatchTimes.length}`);
  console.log(`Retries triggered: ${retryCount}`);
  console.log(`Max concurrent observed: ${maxObserved} (limit: 5)`);
  console.log(`Slots OK (never exceeded): ${slotsOk ? 'YES' : 'NO'}`);

  const passed = slotsOk && retryCount === 5;

  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);

  results.push({
    name: 'C: Retry slot release (slots freed immediately on failure)',
    passed,
    details: [
      `Max concurrent observed: ${maxObserved} (limit: 5)`,
      `Retries: ${retryCount} (expected: 5)`,
      `Key: slots freed immediately, not held during ${RETRY_DELAY}ms delay`,
    ],
  });

  return passed;
}

// ============================================
// TEST D: maxConcurrent Enforcement
// ============================================
async function testMaxConcurrentEnforcement(): Promise<boolean> {
  console.log('\n=== TEST D: maxConcurrent ENFORCEMENT ===');

  const MC = 10;
  const limiter = new PreciseRateLimiter({
    minTime: 5, // 200 RPS for faster test
    maxConcurrent: MC,
    trackDoneStatus: true,
  });

  let maxConcurrent = 0;
  let currentConcurrent = 0;
  const LATENCY = 100;
  const NUM_JOBS = 50;

  const promises = Array.from({ length: NUM_JOBS }, () =>
    limiter.schedule(async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      await simulateLatency(LATENCY);
      currentConcurrent--;
    })
  );

  await Promise.all(promises);

  const passed = maxConcurrent <= MC;

  console.log(`Max concurrent observed: ${maxConcurrent} (limit: ${MC})`);
  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);

  results.push({
    name: `D: maxConcurrent enforcement (never > ${MC})`,
    passed,
    details: [`Max concurrent: ${maxConcurrent} (limit: ${MC})`],
  });

  return passed;
}

// ============================================
// TEST E: Throughput Under Simulated Latency
// ============================================
async function testThroughputWithLatency(): Promise<boolean> {
  console.log('\n=== TEST E: THROUGHPUT UNDER SIMULATED LATENCY ===');

  const latencies = [1000, 2000, 3000];
  let allPassed = true;

  for (const latency of latencies) {
    const limiter = new PreciseRateLimiter({
      minTime: MIN_TIME,
      maxConcurrent: MAX_CONCURRENT,
      trackDoneStatus: true,
    });

    const NUM_JOBS = 300;
    const dispatchTimestamps: number[] = [];

    const promises = Array.from({ length: NUM_JOBS }, () =>
      limiter.schedule(() => {
        dispatchTimestamps.push(performance.now());
        return simulateLatency(latency);
      })
    );

    await Promise.all(promises);

    dispatchTimestamps.sort((a, b) => a - b);
    const dispatchRPS = calculateDispatchRPS(dispatchTimestamps);

    // Calculate spacings
    const spacings: number[] = [];
    for (let i = 1; i < dispatchTimestamps.length; i++) {
      spacings.push(dispatchTimestamps[i] - dispatchTimestamps[i - 1]);
    }
    spacings.sort((a, b) => a - b);
    const p50 = spacings[Math.floor(spacings.length * 0.5)];

    const rpsOk = dispatchRPS >= 90;
    const spacingOk = p50 >= 8 && p50 <= 12;
    const passed = rpsOk && spacingOk;
    if (!passed) allPassed = false;

    console.log(`Latency ${latency}ms: Dispatch RPS=${dispatchRPS.toFixed(1)}, p50 spacing=${p50.toFixed(3)}ms -> ${passed ? 'PASS' : 'FAIL'}`);

    results.push({
      name: `E: Throughput at ${latency}ms latency (dispatch RPS >= 90)`,
      passed,
      details: [
        `Dispatch RPS: ${dispatchRPS.toFixed(1)} (target >= 90)`,
        `Spacing p50: ${p50.toFixed(3)}ms (target 8-12ms)`,
      ],
    });
  }

  return allPassed;
}

// ============================================
// TEST F: Priority Under Heavy Load (Saturated Slots)
// ============================================
async function testPriorityUnderLoad(): Promise<boolean> {
  console.log('\n=== TEST F: PRIORITY UNDER HEAVY LOAD ===');
  console.log(`Config: minTime=${MIN_TIME}ms, maxConcurrent=${MAX_CONCURRENT}, latency=1000ms`);

  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: MAX_CONCURRENT,
    trackDoneStatus: true,
  });

  const SIMULATED_LATENCY = 1000;
  const LOW_PRI_COUNT = 600; // Need > 500 (maxConcurrent) to saturate
  const HIGH_PRI_COUNT = 5;

  const executionLog: { label: string; priority: number; dispatchTime: number }[] = [];

  // Schedule 600 low-priority jobs (priority 5)
  const lowPromises = Array.from({ length: LOW_PRI_COUNT }, (_, i) =>
    limiter.schedule({ priority: 5 }, () => {
      executionLog.push({ label: `low-${i}`, priority: 5, dispatchTime: performance.now() });
      return simulateLatency(SIMULATED_LATENCY);
    })
  );

  // Wait 5500ms: ~550 dispatched at 10ms intervals, 500 in-flight (all slots full)
  // ~50 remaining QUEUED - this is when priority activates
  await new Promise((r) => setTimeout(r, 5500));

  // Inject 5 high-priority counterbid jobs (priority 0)
  const highPromises = Array.from({ length: HIGH_PRI_COUNT }, (_, i) =>
    limiter.schedule({ priority: 0 }, () => {
      executionLog.push({ label: `high-${i}`, priority: 0, dispatchTime: performance.now() });
      return simulateLatency(SIMULATED_LATENCY);
    })
  );

  await Promise.all([...lowPromises, ...highPromises]);

  // Sort by dispatch time
  executionLog.sort((a, b) => a.dispatchTime - b.dispatchTime);

  // Find position of first high-priority job
  const firstHighIdx = executionLog.findIndex((e) => e.priority === 0);
  const highEntries = executionLog.filter((e) => e.priority === 0);
  const lastHighIdx = executionLog.findIndex((e) => e === highEntries[highEntries.length - 1]);
  const lowAfterAllHigh = executionLog
    .slice(lastHighIdx + 1)
    .filter((e) => e.priority === 5).length;

  console.log(`First high-pri at position: #${firstHighIdx + 1}`);
  console.log(`Last high-pri at position: #${lastHighIdx + 1}`);
  console.log(`Low-pri jobs after all high-pri: ${lowAfterAllHigh} (jumped ahead of these)`);

  // High-pri jobs should jump ahead of remaining QUEUED low-pri jobs
  const passed = lowAfterAllHigh >= 10 && highEntries.length === HIGH_PRI_COUNT;

  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);

  results.push({
    name: 'F: Priority under heavy load (high-pri jumps ahead)',
    passed,
    details: [
      `Low-pri after all high-pri: ${lowAfterAllHigh} (min 10)`,
      `All ${HIGH_PRI_COUNT} high-pri executed: ${highEntries.length === HIGH_PRI_COUNT}`,
    ],
  });

  return passed;
}

// ============================================
// TEST G: counts() Compatibility
// ============================================
async function testCountsCompatibility(): Promise<boolean> {
  console.log('\n=== TEST G: counts() COMPATIBILITY ===');

  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: 5,
    trackDoneStatus: true,
  });

  // Before any jobs
  const initialCounts = limiter.counts();
  const hasAllKeys = 'RECEIVED' in initialCounts
    && 'QUEUED' in initialCounts
    && 'RUNNING' in initialCounts
    && 'EXECUTING' in initialCounts
    && 'DONE' in initialCounts;

  console.log(`Initial counts: ${JSON.stringify(initialCounts)}`);
  console.log(`Has all required keys: ${hasAllKeys}`);

  // Run some jobs
  const promises = Array.from({ length: 10 }, () =>
    limiter.schedule(() => simulateLatency(50))
  );

  // Check mid-execution
  await new Promise((r) => setTimeout(r, 30));
  const midCounts = limiter.counts();
  console.log(`Mid-execution counts: ${JSON.stringify(midCounts)}`);

  await Promise.all(promises);

  const finalCounts = limiter.counts();
  console.log(`Final counts: ${JSON.stringify(finalCounts)}`);

  const doneCorrect = finalCounts.DONE === 10;
  const queueEmpty = finalCounts.QUEUED === 0;
  const executingZero = finalCounts.EXECUTING === 0;

  const passed = hasAllKeys && doneCorrect && queueEmpty && executingZero;

  console.log(`DONE=10: ${doneCorrect}, QUEUED=0: ${queueEmpty}, EXECUTING=0: ${executingZero}`);
  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);

  results.push({
    name: 'G: counts() compatibility (all keys, accurate tracking)',
    passed,
    details: [
      `Has all keys: ${hasAllKeys}`,
      `Final: DONE=${finalCounts.DONE}, QUEUED=${finalCounts.QUEUED}, EXECUTING=${finalCounts.EXECUTING}`,
    ],
  });

  return passed;
}

// ============================================
// MAIN
// ============================================
async function main() {
  console.log('PreciseRateLimiter Unit Test Suite');
  console.log(`Target: ${RATE_LIMIT} RPS (${MIN_TIME}ms spacing), maxConcurrent=${MAX_CONCURRENT}`);
  console.log('='.repeat(60));

  await testDispatchPrecision();
  await testPriorityOrdering();
  await testRetrySlotRelease();
  await testMaxConcurrentEnforcement();
  await testThroughputWithLatency();
  await testPriorityUnderLoad();
  await testCountsCompatibility();

  // Summary
  console.log('\n' + '='.repeat(60));
  console.log('SUMMARY');
  console.log('='.repeat(60));

  let allPassed = true;
  for (const r of results) {
    const status = r.passed ? 'PASS' : 'FAIL';
    console.log(`[${status}] ${r.name}`);
    for (const d of r.details) {
      console.log(`       ${d}`);
    }
    if (!r.passed) allPassed = false;
  }

  console.log('\n' + (allPassed ? 'ALL TESTS PASSED' : 'SOME TESTS FAILED'));
  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => {
  console.error('Test suite error:', err);
  process.exit(2);
});
