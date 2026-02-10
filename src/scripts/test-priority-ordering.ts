/**
 * Priority Ordering Test
 *
 * Thoroughly tests Bottleneck priority scheduling across all priority levels
 * and simulates the counterbid+cancel co-scheduling scenario.
 *
 * Uses maxConcurrent=200 (production config) with 1000ms simulated latency
 * to validate priority under realistic conditions.
 *
 * Run with: npx ts-node src/scripts/test-priority-ordering.ts
 */
import { PreciseRateLimiter } from '../utils/precise-rate-limiter';

const RATE_LIMIT = 100;
const MIN_TIME = 10; // 10ms = 100 RPS
const MAX_CONCURRENT = Math.ceil(RATE_LIMIT * 5); // 500
const SIMULATED_LATENCY = 1000; // 1000ms average API latency

interface JobRecord {
  jobType: string;
  priority: number;
  enqueuedAt: number;
  dispatchedAt: number;
  latencyMs: number;
}

interface TestResult {
  name: string;
  passed: boolean;
  details: string[];
}

const results: TestResult[] = [];

// ============================================
// TEST 1: Full Priority Level Ordering
// ============================================
async function testFullPriorityOrdering(): Promise<boolean> {
  console.log('\n=== FULL PRIORITY LEVEL ORDERING TEST ===');
  console.log(`Config: minTime=${MIN_TIME}, maxConcurrent=${MAX_CONCURRENT}, latency=${SIMULATED_LATENCY}ms`);

  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: MAX_CONCURRENT,
    trackDoneStatus: true,
  });

  const PRIORITY_LEVELS = [0, 1, 3, 5, 7, 9];
  const JOBS_PER_LEVEL = 5;

  const records: JobRecord[] = [];

  // Sentinel to block execution while we fill the queue
  let releaseSentinel: () => void;
  const sentinelPromise = new Promise<void>((resolve) => {
    releaseSentinel = resolve;
  });
  const blockerPromise = limiter.schedule({ priority: 0 }, () => sentinelPromise);

  // Wait for sentinel to start executing and queue to settle
  await new Promise((r) => setTimeout(r, 100));

  // Schedule jobs at each priority level (submit in reverse order to test sorting)
  const allPromises: Promise<void>[] = [];
  const reversedLevels = [...PRIORITY_LEVELS].reverse(); // 9, 7, 5, 3, 1, 0

  for (const priority of reversedLevels) {
    for (let i = 0; i < JOBS_PER_LEVEL; i++) {
      const jobType = `p${priority}-job${i}`;
      const enqueuedAt = performance.now();

      allPromises.push(
        limiter.schedule({ priority }, () => {
          const dispatchedAt = performance.now();
          records.push({
            jobType,
            priority,
            enqueuedAt,
            dispatchedAt,
            latencyMs: dispatchedAt - enqueuedAt,
          });
          return new Promise<void>((resolve) => setTimeout(resolve, SIMULATED_LATENCY));
        })
      );
    }
  }

  // Release the sentinel so queued jobs start draining in priority order
  releaseSentinel!();
  await blockerPromise;
  await Promise.all(allPromises);

  // Sort records by dispatchedAt to get actual execution order
  const ordered = [...records].sort((a, b) => a.dispatchedAt - b.dispatchedAt);

  console.log('Execution order:');
  for (const r of ordered) {
    console.log(
      `  ${r.jobType.padEnd(12)} | priority=${r.priority} | latency=${r.latencyMs.toFixed(1)}ms`
    );
  }

  // Verify: for each pair of adjacent priority levels, all jobs of the
  // lower-numbered (higher) priority should execute before the higher-numbered ones
  let orderingCorrect = true;
  const violations: string[] = [];

  for (let li = 0; li < PRIORITY_LEVELS.length - 1; li++) {
    const higherPri = PRIORITY_LEVELS[li];
    const lowerPri = PRIORITY_LEVELS[li + 1];

    const higherPositions = ordered
      .map((r, idx) => (r.priority === higherPri ? idx : -1))
      .filter((idx) => idx >= 0);
    const lowerPositions = ordered
      .map((r, idx) => (r.priority === lowerPri ? idx : -1))
      .filter((idx) => idx >= 0);

    if (higherPositions.length === 0 || lowerPositions.length === 0) continue;

    const lastHigher = Math.max(...higherPositions);
    const firstLower = Math.min(...lowerPositions);

    if (lastHigher > firstLower) {
      orderingCorrect = false;
      violations.push(
        `Priority ${higherPri} job ran at position ${lastHigher} after priority ${lowerPri} job at position ${firstLower}`
      );
    }
  }

  // With PreciseRateLimiter, priority ordering is always enforced via the min-heap.
  // All queued jobs are sorted by priority regardless of maxConcurrent.
  const passed = orderingCorrect;
  console.log(`\nOrdering correct: ${orderingCorrect}`);
  console.log(`Violations: ${violations.length}`);
  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);

  if (violations.length > 0) {
    violations.forEach((v) => console.log(`  Violation: ${v}`));
  }

  results.push({
    name: `Full priority ordering (6 levels, maxConcurrent=${MAX_CONCURRENT})`,
    passed,
    details: orderingCorrect
      ? ['All 6 priority levels executed in correct order']
      : [
          ...violations,
          `${violations.length} violations tolerated (slots available, FIFO dispatch expected)`,
        ],
  });

  return passed;
}

// ============================================
// TEST 2: Counterbid + Cancel Co-scheduling
// ============================================
async function testCounterbidCancelLatency(): Promise<boolean> {
  console.log('\n=== COUNTERBID + CANCEL CO-SCHEDULING TEST ===');
  console.log(`Config: minTime=${MIN_TIME}, maxConcurrent=${MAX_CONCURRENT}, latency=${SIMULATED_LATENCY}ms`);

  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: MAX_CONCURRENT,
    trackDoneStatus: true,
  });

  const records: JobRecord[] = [];
  const QUEUE_FILL = 50;

  // Sentinel to block execution while we fill queue
  let releaseSentinel: () => void;
  const sentinelPromise = new Promise<void>((resolve) => {
    releaseSentinel = resolve;
  });
  const blockerPromise = limiter.schedule({ priority: 0 }, () => sentinelPromise);

  // Wait for sentinel to start
  await new Promise((r) => setTimeout(r, 20));

  // Fill queue with 50 priority-5 jobs (with simulated latency)
  const fillPromises = Array.from({ length: QUEUE_FILL }, (_, i) => {
    const enqueuedAt = performance.now();
    return limiter.schedule({ priority: 5 }, () => {
      const dispatchedAt = performance.now();
      records.push({
        jobType: `fill-${i}`,
        priority: 5,
        enqueuedAt,
        dispatchedAt,
        latencyMs: dispatchedAt - enqueuedAt,
      });
      return new Promise<void>((resolve) => setTimeout(resolve, SIMULATED_LATENCY));
    });
  });

  // Add priority-0 counterbid + cancel jobs (with simulated latency)
  const bidEnqueuedAt = performance.now();
  const bidPromise = limiter.schedule({ priority: 0 }, () => {
    const dispatchedAt = performance.now();
    records.push({
      jobType: 'counterbid',
      priority: 0,
      enqueuedAt: bidEnqueuedAt,
      dispatchedAt,
      latencyMs: dispatchedAt - bidEnqueuedAt,
    });
    return new Promise<void>((resolve) => setTimeout(resolve, SIMULATED_LATENCY));
  });

  const cancelEnqueuedAt = performance.now();
  const cancelPromise = limiter.schedule({ priority: 0 }, () => {
    const dispatchedAt = performance.now();
    records.push({
      jobType: 'cancel',
      priority: 0,
      enqueuedAt: cancelEnqueuedAt,
      dispatchedAt,
      latencyMs: dispatchedAt - cancelEnqueuedAt,
    });
    return new Promise<void>((resolve) => setTimeout(resolve, SIMULATED_LATENCY));
  });

  // Release sentinel -- jobs drain in priority order
  releaseSentinel!();
  await blockerPromise;
  await Promise.all([...fillPromises, bidPromise, cancelPromise]);

  // Find the counterbid and cancel records
  const bidRecord = records.find((r) => r.jobType === 'counterbid')!;
  const cancelRecord = records.find((r) => r.jobType === 'cancel')!;

  console.log('\nInstrumentation data (priority-0 jobs):');
  console.log(
    `  counterbid | priority=${bidRecord.priority} | enqueuedAt=${bidRecord.enqueuedAt.toFixed(1)} | dispatchedAt=${bidRecord.dispatchedAt.toFixed(1)} | latency=${bidRecord.latencyMs.toFixed(1)}ms`
  );
  console.log(
    `  cancel     | priority=${cancelRecord.priority} | enqueuedAt=${cancelRecord.enqueuedAt.toFixed(1)} | dispatchedAt=${cancelRecord.dispatchedAt.toFixed(1)} | latency=${cancelRecord.latencyMs.toFixed(1)}ms`
  );

  // Calculate spacing from all jobs
  const allDispatched = records
    .map((r) => r.dispatchedAt)
    .sort((a, b) => a - b);
  const spacings: number[] = [];
  for (let i = 1; i < allDispatched.length; i++) {
    spacings.push(allDispatched[i] - allDispatched[i - 1]);
  }
  spacings.sort((a, b) => a - b);

  const p50 = spacings[Math.floor(spacings.length * 0.5)];
  const p99 = spacings[Math.floor(spacings.length * 0.99)];

  console.log(`\nSpacing: p50=${p50.toFixed(2)}ms | p99=${p99.toFixed(2)}ms`);
  console.log(`Counterbid latency: ${bidRecord.latencyMs.toFixed(1)}ms`);
  console.log(`Cancel latency: ${cancelRecord.latencyMs.toFixed(1)}ms`);

  // Check burst in 100ms windows
  let maxBurst = 0;
  for (let i = 0; i < allDispatched.length; i++) {
    const windowEnd = allDispatched[i] + 100;
    let count = 0;
    for (let j = i; j < allDispatched.length && allDispatched[j] < windowEnd; j++) {
      count++;
    }
    maxBurst = Math.max(maxBurst, count);
  }
  console.log(`Max burst in 100ms window: ${maxBurst}`);

  // Verify execution order: both p0 jobs should run before most p5 jobs
  const ordered = [...records].sort((a, b) => a.dispatchedAt - b.dispatchedAt);
  const bidIndex = ordered.findIndex((r) => r.jobType === 'counterbid');
  const cancelIndex = ordered.findIndex((r) => r.jobType === 'cancel');
  const p5AfterCancel = ordered.filter(
    (r, idx) => r.priority === 5 && idx > Math.max(bidIndex, cancelIndex)
  ).length;

  console.log(
    `\nExecution position: counterbid=#${bidIndex + 1}, cancel=#${cancelIndex + 1} (of ${ordered.length})`
  );
  console.log(`Priority-5 jobs after both p0 jobs: ${p5AfterCancel}/${QUEUE_FILL}`);

  // With PreciseRateLimiter, priority-0 jobs jump ahead of queued priority-5 jobs.
  const TARGET_LATENCY_MS = 1000;
  const bidLatencyOk = bidRecord.latencyMs < TARGET_LATENCY_MS;
  const cancelLatencyOk = cancelRecord.latencyMs < TARGET_LATENCY_MS;
  // With 1000ms latency, spacing between dispatches should still be close to minTime
  // when slots are available (200 slots, only 52 jobs).
  const spacingOk = p50 >= 8 && p50 <= 14;
  const spacingP99Ok = p99 < 250;
  const burstOk = maxBurst <= 12;
  // With PreciseRateLimiter, p0 jobs jump ahead of queued p5 jobs
  const orderOk = p5AfterCancel >= 20; // At least 20 fill jobs after p0 jobs

  const passed = bidLatencyOk && cancelLatencyOk && orderOk;

  console.log(`\nRESULT: ${passed ? 'PASS' : 'FAIL'}`);

  const details: string[] = [
    `Counterbid latency: ${bidRecord.latencyMs.toFixed(1)}ms (target <${TARGET_LATENCY_MS}ms) ${bidLatencyOk ? '' : '-- FAIL'}`,
    `Cancel latency: ${cancelRecord.latencyMs.toFixed(1)}ms (target <${TARGET_LATENCY_MS}ms) ${cancelLatencyOk ? '' : '-- FAIL'}`,
    `Priority ordering: ${p5AfterCancel}/${QUEUE_FILL} p5 jobs ran after p0 jobs (FIFO expected with slots available)`,
    `Config: maxConcurrent=${MAX_CONCURRENT}, latency=${SIMULATED_LATENCY}ms`,
  ];

  if (!bidLatencyOk) details.push(`Counterbid latency exceeded ${TARGET_LATENCY_MS}ms`);
  if (!cancelLatencyOk) details.push(`Cancel latency exceeded ${TARGET_LATENCY_MS}ms`);

  results.push({
    name: `Counterbid+cancel latency (<${TARGET_LATENCY_MS}ms)`,
    passed,
    details,
  });

  // Record spacing results
  results.push({
    name: 'Co-scheduling spacing (p50 ~10ms)',
    passed: spacingOk && spacingP99Ok,
    details: [
      `p50=${p50.toFixed(2)}ms (target 8-14ms) ${spacingOk ? '' : '-- FAIL'}`,
      `p99=${p99.toFixed(2)}ms (target <250ms) ${spacingP99Ok ? '' : '-- FAIL'}`,
    ],
  });

  results.push({
    name: 'Co-scheduling burst (max 12 per 100ms)',
    passed: burstOk,
    details: [`Max burst: ${maxBurst} (target <=12) ${burstOk ? '' : '-- FAIL'}`],
  });

  return passed;
}

// ============================================
// TEST 3: Production Config Validation (maxConcurrent=200 with latency)
// ============================================
async function testProductionConfigValidation(): Promise<void> {
  console.log(`\n=== VALIDATION: PRODUCTION CONFIG (maxConcurrent=${MAX_CONCURRENT}, ${SIMULATED_LATENCY}ms latency) ===`);
  console.log('This test validates the current production config supports priority under heavy load.');

  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: MAX_CONCURRENT,
    trackDoneStatus: true,
  });

  const records: JobRecord[] = [];
  const QUEUE_FILL = 400; // Need enough so that after 2500ms (~250 dispatched), ~150 are still QUEUED

  // Fill queue with priority-5 jobs (with simulated latency)
  const fillPromises = Array.from({ length: QUEUE_FILL }, (_, i) => {
    const enqueuedAt = performance.now();
    return limiter.schedule({ priority: 5 }, () => {
      const dispatchedAt = performance.now();
      records.push({
        jobType: `fill-${i}`,
        priority: 5,
        enqueuedAt,
        dispatchedAt,
        latencyMs: dispatchedAt - enqueuedAt,
      });
      return new Promise<void>((resolve) => setTimeout(resolve, SIMULATED_LATENCY));
    });
  });

  // Wait for slots to fill (~2500ms = ~250 jobs dispatched at 10ms intervals, 200 in-flight)
  // At this point: ~200 in-flight, ~50 completed, ~150 still QUEUED
  await new Promise((r) => setTimeout(r, 2500));

  // Add priority-0 jobs (with simulated latency)
  const bidEnqueuedAt = performance.now();
  const bidPromise = limiter.schedule({ priority: 0 }, () => {
    const dispatchedAt = performance.now();
    records.push({
      jobType: 'counterbid',
      priority: 0,
      enqueuedAt: bidEnqueuedAt,
      dispatchedAt,
      latencyMs: dispatchedAt - bidEnqueuedAt,
    });
    return new Promise<void>((resolve) => setTimeout(resolve, SIMULATED_LATENCY));
  });

  const cancelEnqueuedAt = performance.now();
  const cancelPromise = limiter.schedule({ priority: 0 }, () => {
    const dispatchedAt = performance.now();
    records.push({
      jobType: 'cancel',
      priority: 0,
      enqueuedAt: cancelEnqueuedAt,
      dispatchedAt,
      latencyMs: dispatchedAt - cancelEnqueuedAt,
    });
    return new Promise<void>((resolve) => setTimeout(resolve, SIMULATED_LATENCY));
  });

  await Promise.all([...fillPromises, bidPromise, cancelPromise]);

  const bidRecord = records.find((r) => r.jobType === 'counterbid')!;
  const cancelRecord = records.find((r) => r.jobType === 'cancel')!;

  const ordered = [...records].sort((a, b) => a.dispatchedAt - b.dispatchedAt);
  const bidIdx = ordered.findIndex((r) => r.jobType === 'counterbid');
  const cancelIdx = ordered.findIndex((r) => r.jobType === 'cancel');

  // Count how many fill jobs were dispatched before the counterbid
  const fillBeforeBid = ordered.slice(0, bidIdx).filter((r) => r.priority === 5).length;
  const fillAfterBid = ordered.slice(bidIdx).filter((r) => r.priority === 5).length;

  console.log(`Counterbid executed at position #${bidIdx + 1} of ${ordered.length}`);
  console.log(`Cancel executed at position #${cancelIdx + 1} of ${ordered.length}`);
  console.log(`Counterbid latency: ${bidRecord.latencyMs.toFixed(1)}ms`);
  console.log(`Cancel latency: ${cancelRecord.latencyMs.toFixed(1)}ms`);
  console.log(`Fill jobs before counterbid: ${fillBeforeBid} | after: ${fillAfterBid}`);

  // With PreciseRateLimiter, priority always works (min-heap ordering).
  // After 2500ms at 10ms spacing, ~250 dispatched. ~150 still queued.
  // Priority-0 counterbid jumps ahead of queued p5 jobs.
  const priorityWorking = fillAfterBid >= 50; // At least 50 fill jobs ran after counterbid

  results.push({
    name: 'VALIDATION: Production config priority under heavy load',
    passed: priorityWorking,
    details: [
      `Counterbid at position #${bidIdx + 1} of ${ordered.length}`,
      `Fill jobs after counterbid: ${fillAfterBid} (expect >= 50 — jumped ahead of QUEUED p5 jobs)`,
      `Cancel at position #${cancelIdx + 1}`,
      `Latency: counterbid=${bidRecord.latencyMs.toFixed(1)}ms cancel=${cancelRecord.latencyMs.toFixed(1)}ms`,
      priorityWorking
        ? 'CONFIRMED: Production config supports priority ordering under heavy load'
        : 'UNEXPECTED: Priority not working with maxConcurrent set',
    ],
  });
}

// ============================================
// MAIN
// ============================================
async function main() {
  console.log('Priority Ordering Test Suite (PreciseRateLimiter)');
  console.log(`Limiter: minTime=${MIN_TIME}ms (${1000 / MIN_TIME} RPS), maxConcurrent=${MAX_CONCURRENT}`);
  console.log('='.repeat(60));

  // Test 1: Full priority ordering
  await testFullPriorityOrdering();

  // Test 2: Counterbid+cancel scenario
  await testCounterbidCancelLatency();

  // Test 3: Validate production config supports priority under heavy load
  await testProductionConfigValidation();

  // Acceptance Criteria Summary
  console.log('\n' + '='.repeat(60));
  console.log('ACCEPTANCE CRITERIA SUMMARY');
  console.log('='.repeat(60));

  for (const r of results) {
    const status = r.passed ? 'PASS' : 'FAIL';
    console.log(`[${status}] ${r.name}`);
    for (const d of r.details) {
      console.log(`       ${d}`);
    }
  }

  const allPassed = results.every((r) => r.passed);

  console.log('\n' + '='.repeat(60));
  console.log('PRODUCTION CONFIG (PreciseRateLimiter):');
  console.log(`  maxConcurrent = ceil(rateLimit * 5) = ${MAX_CONCURRENT}`);
  console.log(`  Supports full throughput at ${SIMULATED_LATENCY}ms latency AND priority via min-heap.`);
  console.log(`  Priority always works: min-heap ensures p0 jobs jump ahead of queued p5 jobs.`);
  console.log('='.repeat(60));

  console.log(
    '\n' + (allPassed ? 'ALL TESTS PASSED' : 'SOME TESTS FAILED')
  );
  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => {
  console.error('Test suite error:', err);
  process.exit(2);
});
