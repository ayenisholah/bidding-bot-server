/**
 * Rate Pacing Test
 *
 * Verifies PreciseRateLimiter produces correct spacing at 100 RPS
 * and tests priority ordering behavior.
 *
 * PreciseRateLimiter always enforces priority via min-heap, regardless
 * of maxConcurrent setting. This is an improvement over Bottleneck which
 * required maxConcurrent for priority to take effect.
 *
 * Run with: npx ts-node src/scripts/test-rate-pacing.ts
 */
import { PreciseRateLimiter } from '../utils/precise-rate-limiter';

const RATE_LIMIT = 100;
const MIN_TIME = 1000 / RATE_LIMIT; // 10ms
const MAX_CONCURRENT = Math.ceil(RATE_LIMIT * 5); // 500 (production config)

interface TestResult {
  name: string;
  passed: boolean;
  details: string[];
}

const results: TestResult[] = [];

// ============================================
// TEST 1: Spacing Test
// ============================================
async function testSpacing(): Promise<{ passed: boolean; timestamps: number[] }> {
  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: MAX_CONCURRENT,
    trackDoneStatus: true,
  });

  const timestamps: number[] = [];
  const NUM_JOBS = 200;

  // Schedule all jobs
  const promises = Array.from({ length: NUM_JOBS }, (_, _i) =>
    limiter.schedule(() => {
      timestamps.push(performance.now());
      return Promise.resolve();
    })
  );

  await Promise.all(promises);

  // Calculate spacings
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

  console.log('\n=== SPACING TEST ===');
  console.log(`Jobs: ${NUM_JOBS}, Target spacing: ${MIN_TIME}ms`);
  console.log(`p50: ${p50.toFixed(2)}ms | p95: ${p95.toFixed(2)}ms | p99: ${p99.toFixed(2)}ms`);
  console.log(`min: ${min.toFixed(2)}ms | max: ${max.toFixed(2)}ms`);

  const details: string[] = [];
  let passed = true;

  if (p50 < 8 || p50 > 12) {
    passed = false;
    details.push(`p50 ${p50.toFixed(2)}ms outside 10ms +/- 2ms`);
  }
  if (p99 >= 20) {
    passed = false;
    details.push(`p99 ${p99.toFixed(2)}ms exceeds 20ms`);
  }

  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);
  details.forEach((d) => console.log(`  ${d}`));

  results.push({
    name: 'Spacing (p50 ~10ms, p99 <20ms)',
    passed,
    details: [
      `p50=${p50.toFixed(2)}ms p95=${p95.toFixed(2)}ms p99=${p99.toFixed(2)}ms`,
      `min=${min.toFixed(2)}ms max=${max.toFixed(2)}ms`,
      ...details,
    ],
  });

  return { passed, timestamps };
}

// ============================================
// TEST 2: Burst Detection Test
// ============================================
function testBurstDetection(timestamps: number[]): boolean {
  console.log('\n=== BURST DETECTION TEST ===');

  const WINDOW_MS = 100;
  const MAX_ALLOWED = 12; // 10 expected + 20% tolerance

  let maxBurst = 0;
  let maxBurstStart = 0;

  // Sliding window: for each timestamp, count how many fall within [t, t+100ms)
  for (let i = 0; i < timestamps.length; i++) {
    const windowStart = timestamps[i];
    const windowEnd = windowStart + WINDOW_MS;
    let count = 0;
    for (let j = i; j < timestamps.length && timestamps[j] < windowEnd; j++) {
      count++;
    }
    if (count > maxBurst) {
      maxBurst = count;
      maxBurstStart = windowStart;
    }
  }

  const passed = maxBurst <= MAX_ALLOWED;

  console.log(`Window: ${WINDOW_MS}ms | Max allowed: ${MAX_ALLOWED} requests`);
  console.log(`Max burst observed: ${maxBurst} requests`);
  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);

  const details: string[] = [`Max burst: ${maxBurst} in ${WINDOW_MS}ms window`];
  if (!passed) {
    details.push(
      `Exceeded limit of ${MAX_ALLOWED} at offset ${(maxBurstStart - timestamps[0]).toFixed(1)}ms`
    );
  }

  results.push({
    name: `Burst detection (max ${MAX_ALLOWED} per ${WINDOW_MS}ms)`,
    passed,
    details,
  });

  return passed;
}

// ============================================
// TEST 3a: Priority Ordering - Instant Jobs (no latency)
// ============================================
async function testPriorityInstantJobs(): Promise<boolean> {
  console.log('\n=== PRIORITY TEST: INSTANT JOBS (no latency) ===');
  console.log('PreciseRateLimiter always enforces priority via min-heap.');

  const limiter = new PreciseRateLimiter({
    minTime: 10,
    maxConcurrent: MAX_CONCURRENT,
    trackDoneStatus: true,
  });

  const executionLog: { label: string; priority: number; index: number }[] = [];
  let execOrder = 0;

  // Use a sentinel to hold execution while we fill the queue
  let releaseSentinel: () => void;
  const sentinelPromise = new Promise<void>((resolve) => {
    releaseSentinel = resolve;
  });
  const blockerPromise = limiter.schedule({ priority: 0 }, () => sentinelPromise);

  // Wait for sentinel to start executing and queue to settle
  await new Promise((r) => setTimeout(r, 100));

  // Schedule 50 low-priority jobs (priority 5)
  const lowPriorityPromises = Array.from({ length: 50 }, (_, i) =>
    limiter.schedule({ priority: 5 }, () => {
      executionLog.push({ label: `low-${i}`, priority: 5, index: execOrder++ });
      return Promise.resolve();
    })
  );

  // Schedule 10 high-priority jobs (priority 0)
  const highPriorityPromises = Array.from({ length: 10 }, (_, i) =>
    limiter.schedule({ priority: 0 }, () => {
      executionLog.push({ label: `high-${i}`, priority: 0, index: execOrder++ });
      return Promise.resolve();
    })
  );

  // Release sentinel -- jobs should drain in priority order (high first)
  releaseSentinel!();
  await blockerPromise;
  await Promise.all([...lowPriorityPromises, ...highPriorityPromises]);

  const highEntries = executionLog.filter((e) => e.priority === 0);
  const lowBeforeFirstHigh = executionLog.filter(
    (e) => e.priority === 5 && e.index < Math.min(...highEntries.map((h) => h.index))
  );

  console.log(`High-priority jobs: ${highEntries.length}`);
  console.log(`Low-priority jobs before first high: ${lowBeforeFirstHigh.length}`);
  console.log(
    `First 15 executed: ${executionLog
      .slice(0, 15)
      .map((e) => e.label)
      .join(', ')}`
  );

  // PreciseRateLimiter uses min-heap: high-pri (0) should execute before low-pri (5)
  // Allow at most 2 low-pri jobs to sneak through due to timing
  const passed = lowBeforeFirstHigh.length <= 2 && highEntries.length === 10;

  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);

  results.push({
    name: 'Priority: instant jobs (high-pri before low-pri)',
    passed,
    details: [
      `${lowBeforeFirstHigh.length} low-pri jobs ran before first high-pri (max 2 allowed)`,
      `${highEntries.length}/10 high-pri jobs executed`,
      'PreciseRateLimiter always enforces priority via min-heap',
    ],
  });

  return passed;
}

// ============================================
// TEST 3b: Priority Ordering - With Simulated Latency (production-like)
// ============================================
async function testPriorityWithLatency(): Promise<boolean> {
  console.log(`\n=== PRIORITY TEST: WITH LATENCY (maxConcurrent=${MAX_CONCURRENT}) ===`);
  console.log('Includes 1000ms simulated latency for realistic conditions.');

  const limiter = new PreciseRateLimiter({
    minTime: 10,
    maxConcurrent: MAX_CONCURRENT,
    trackDoneStatus: true,
  });

  const SIMULATED_LATENCY = 1000;
  const executionLog: { label: string; priority: number; index: number }[] = [];
  let execOrder = 0;

  // Sentinel to block execution while we fill the queue
  let releaseSentinel: () => void;
  const sentinelPromise = new Promise<void>((resolve) => {
    releaseSentinel = resolve;
  });
  const blockerPromise = limiter.schedule({ priority: 0 }, () => sentinelPromise);

  // Wait for sentinel to start executing and queue to settle
  await new Promise((r) => setTimeout(r, 100));

  // Schedule 50 low-priority jobs (priority 5) with simulated latency
  const lowPriorityPromises = Array.from({ length: 50 }, (_, i) =>
    limiter.schedule({ priority: 5 }, () => {
      executionLog.push({ label: `low-${i}`, priority: 5, index: execOrder++ });
      return new Promise<void>((resolve) => setTimeout(resolve, SIMULATED_LATENCY));
    })
  );

  // Schedule 10 high-priority jobs (priority 0) with simulated latency
  const highPriorityPromises = Array.from({ length: 10 }, (_, i) =>
    limiter.schedule({ priority: 0 }, () => {
      executionLog.push({ label: `high-${i}`, priority: 0, index: execOrder++ });
      return new Promise<void>((resolve) => setTimeout(resolve, SIMULATED_LATENCY));
    })
  );

  // Release sentinel -- jobs should drain in priority order
  releaseSentinel!();
  await blockerPromise;
  await Promise.all([...lowPriorityPromises, ...highPriorityPromises]);

  const highEntries = executionLog.filter((e) => e.priority === 0);
  const lowEntries = executionLog.filter((e) => e.priority === 5);

  const lowBeforeFirstHigh = lowEntries.filter(
    (e) => e.index < Math.min(...highEntries.map((h) => h.index))
  );

  console.log(`High-priority jobs: ${highEntries.length}`);
  console.log(`Low-priority jobs before first high: ${lowBeforeFirstHigh.length}`);
  console.log(
    `First 15 executed: ${executionLog
      .slice(0, 15)
      .map((e) => e.label)
      .join(', ')}`
  );

  // PreciseRateLimiter dispatches from min-heap in priority order.
  // High-pri (0) should execute before low-pri (5). Allow small leakage.
  const passed = lowBeforeFirstHigh.length <= 2 && highEntries.length === 10;

  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);

  const details: string[] = [
    `${lowBeforeFirstHigh.length} low-pri jobs ran before first high-pri (max 2 allowed)`,
    `${highEntries.length}/10 high-pri jobs executed`,
    `Config: maxConcurrent=${MAX_CONCURRENT}, simulated latency=${SIMULATED_LATENCY}ms`,
  ];

  if (!passed) {
    if (lowBeforeFirstHigh.length > 2) {
      details.push(
        `Too many low-priority jobs (${lowBeforeFirstHigh.length}) ran before high-priority`
      );
    }
  }

  results.push({
    name: `Priority ordering with latency (p0 before p5)`,
    passed,
    details,
  });

  return passed;
}

// ============================================
// MAIN
// ============================================
async function main() {
  console.log('Rate Pacing & Priority Test Suite (PreciseRateLimiter)');
  console.log(`Target: ${RATE_LIMIT} RPS (${MIN_TIME}ms spacing), maxConcurrent=${MAX_CONCURRENT}`);
  console.log('='.repeat(60));

  // Test 1: Spacing
  const { timestamps } = await testSpacing();

  // Test 2: Burst detection (uses timestamps from test 1)
  testBurstDetection(timestamps);

  // Test 3a: Priority with instant jobs
  await testPriorityInstantJobs();

  // Test 3b: Priority with simulated latency
  await testPriorityWithLatency();

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

  console.log('\n' + '='.repeat(60));
  console.log('NOTES:');
  console.log('  PreciseRateLimiter always enforces priority via min-heap.');
  console.log('  Test 3a: Confirms priority works with instant (zero-latency) jobs.');
  console.log('  Test 3b: Confirms priority works under realistic 1000ms latency.');
  console.log(`  Production config: maxConcurrent=${MAX_CONCURRENT} (5x rate limit).`);
  console.log('='.repeat(60));

  console.log('\n' + (allPassed ? 'ALL TESTS PASSED' : 'SOME TESTS FAILED'));
  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => {
  console.error('Test suite error:', err);
  process.exit(2);
});
