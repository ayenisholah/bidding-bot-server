/**
 * Counterbid End-to-End Latency Test
 *
 * Measures event-to-dispatch latency for the direct counterbid dispatch path.
 * Simulates the flow: event received → dedup check → limiter.schedule() → dispatch
 *
 * Target: < 15ms p99 latency
 *
 * Run with: npx ts-node src/scripts/test-counterbid-e2e-latency.ts
 */
import { PreciseRateLimiter } from '../utils/precise-rate-limiter';
import { LRUCache } from 'lru-cache';

const RATE_LIMIT = 100;
const MIN_TIME = 1000 / RATE_LIMIT; // 10ms
const MAX_CONCURRENT = Math.ceil(RATE_LIMIT * 5); // 500
const SIMULATED_LATENCY = 1000;

interface TestResult {
  name: string;
  passed: boolean;
  details: string[];
}

const results: TestResult[] = [];

function simulateLatency(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================
// Simulated direct dispatch path
// ============================================
function createDirectDispatcher() {
  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: MAX_CONCURRENT,
    trackDoneStatus: true,
  });

  const dedupCache = new LRUCache<string, number>({ max: 10000, ttl: 5000 });
  const latencies: number[] = [];

  function dispatchDirect(dedupKey: string, eventTime: number): Promise<void> {
    // Dedup check
    if (dedupCache.has(dedupKey)) return Promise.resolve();
    dedupCache.set(dedupKey, Date.now());

    return limiter.schedule({ priority: 0 }, async () => {
      // Record latency: event time → dispatch time
      const dispatchTime = performance.now();
      latencies.push(dispatchTime - eventTime);
      // Simulate API call
      await simulateLatency(SIMULATED_LATENCY);
    }).catch(() => {});
  }

  return { limiter, dispatchDirect, latencies };
}

// ============================================
// TEST A: Direct Dispatch Latency Under No Load
// ============================================
async function testNoLoadLatency(): Promise<boolean> {
  console.log('\n=== TEST A: DIRECT DISPATCH LATENCY (NO LOAD) ===');
  console.log('Measures latency with no competing jobs');

  const { dispatchDirect, latencies } = createDirectDispatcher();
  const NUM_COUNTERBIDS = 20;

  // Send counterbids one at a time, waiting for each to dispatch
  for (let i = 0; i < NUM_COUNTERBIDS; i++) {
    const eventTime = performance.now();
    await dispatchDirect(`cb-${i}`, eventTime);
  }

  // Wait for all to complete
  await simulateLatency(SIMULATED_LATENCY + 500);

  latencies.sort((a, b) => a - b);
  const p50 = latencies[Math.floor(latencies.length * 0.5)];
  const p95 = latencies[Math.floor(latencies.length * 0.95)];
  const p99 = latencies[Math.floor(latencies.length * 0.99)];

  console.log(`Counterbids: ${latencies.length}`);
  console.log(`Latency: p50=${p50.toFixed(2)}ms | p95=${p95.toFixed(2)}ms | p99=${p99.toFixed(2)}ms`);

  // With no load, latency should just be the timing interval (0-10ms)
  const passed = p99 < 15;

  console.log(`p99 < 15ms: ${passed ? 'PASS' : 'FAIL'}`);

  results.push({
    name: 'A: Direct dispatch latency (no load, p99 < 15ms)',
    passed,
    details: [
      `p50=${p50.toFixed(2)}ms p95=${p95.toFixed(2)}ms p99=${p99.toFixed(2)}ms`,
    ],
  });

  return passed;
}

// ============================================
// TEST B: Direct Dispatch Latency Under Steady Load
// ============================================
async function testSteadyLoadLatency(): Promise<boolean> {
  console.log('\n=== TEST B: DIRECT DISPATCH LATENCY (STEADY LOAD) ===');
  console.log(`Config: ${RATE_LIMIT} RPS steady load, ${SIMULATED_LATENCY}ms latency`);

  const { limiter, dispatchDirect, latencies } = createDirectDispatcher();

  // Start a continuous stream of priority-5 background jobs
  const STEADY_STATE_JOBS = 200;
  const bgPromises = Array.from({ length: STEADY_STATE_JOBS }, () =>
    limiter.schedule({ priority: 5 }, () => simulateLatency(SIMULATED_LATENCY))
  );

  // Wait for steady state (~1500ms = 150 dispatched, ~100 in-flight)
  await new Promise((r) => setTimeout(r, 1500));

  // Now inject priority-0 counterbids and measure latency
  const NUM_COUNTERBIDS = 10;
  const cbPromises: Promise<void>[] = [];

  for (let i = 0; i < NUM_COUNTERBIDS; i++) {
    const eventTime = performance.now();
    cbPromises.push(dispatchDirect(`cb-steady-${i}`, eventTime));
    // Stagger slightly
    await new Promise((r) => setTimeout(r, 20));
  }

  await Promise.all(cbPromises);
  await Promise.all(bgPromises);

  latencies.sort((a, b) => a - b);
  const p50 = latencies[Math.floor(latencies.length * 0.5)];
  const p95 = latencies[Math.floor(latencies.length * 0.95)];
  const p99 = latencies[Math.floor(latencies.length * 0.99)];

  console.log(`Counterbids: ${latencies.length}`);
  console.log(`Latency: p50=${p50.toFixed(2)}ms | p95=${p95.toFixed(2)}ms | p99=${p99.toFixed(2)}ms`);

  // Under steady load with slots available, latency = wait for next timing slot (~10ms)
  const passed = p99 < 15;

  console.log(`p99 < 15ms: ${passed ? 'PASS' : 'FAIL'}`);

  results.push({
    name: 'B: Direct dispatch latency (steady load, p99 < 15ms)',
    passed,
    details: [
      `p50=${p50.toFixed(2)}ms p95=${p95.toFixed(2)}ms p99=${p99.toFixed(2)}ms`,
      `Background jobs: ${STEADY_STATE_JOBS} priority-5, ${SIMULATED_LATENCY}ms latency`,
    ],
  });

  return passed;
}

// ============================================
// TEST C: Dedup Effectiveness
// ============================================
async function testDedupEffectiveness(): Promise<boolean> {
  console.log('\n=== TEST C: DEDUP EFFECTIVENESS ===');

  const { dispatchDirect, latencies } = createDirectDispatcher();

  // Send same dedup key 10 times
  const eventTime = performance.now();
  for (let i = 0; i < 10; i++) {
    await dispatchDirect('same-key', eventTime);
  }

  // Send 5 unique keys
  for (let i = 0; i < 5; i++) {
    const t = performance.now();
    await dispatchDirect(`unique-${i}`, t);
  }

  await simulateLatency(SIMULATED_LATENCY + 500);

  // Only 6 should have dispatched (1 for same-key + 5 unique)
  const dispatched = latencies.length;
  const passed = dispatched === 6;

  console.log(`Dispatched: ${dispatched} (expected: 6)`);
  console.log(`Dedup blocked: ${10 - 1} duplicate dispatches`);
  console.log(`RESULT: ${passed ? 'PASS' : 'FAIL'}`);

  results.push({
    name: 'C: Dedup effectiveness (9 duplicates blocked)',
    passed,
    details: [
      `Dispatched: ${dispatched} (expected: 6)`,
      `Same-key sent 10 times, only 1 dispatched`,
    ],
  });

  return passed;
}

// ============================================
// TEST D: Single Counterbid Under Saturated Load
// ============================================
async function testPriorityJumpLatency(): Promise<boolean> {
  console.log('\n=== TEST D: SINGLE COUNTERBID LATENCY (SATURATED) ===');
  console.log(`Config: maxConcurrent=${MAX_CONCURRENT}, latency=${SIMULATED_LATENCY}ms`);
  console.log('Simulates realistic scenario: one counterbid arrives during heavy load.');

  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: MAX_CONCURRENT,
    trackDoneStatus: true,
  });

  const latencies: number[] = [];

  // Fill all slots: need > maxConcurrent jobs in-flight
  const BG_COUNT = 800;
  const bgPromises = Array.from({ length: BG_COUNT }, () =>
    limiter.schedule({ priority: 5 }, () => simulateLatency(SIMULATED_LATENCY))
  );

  // Wait for slots to fill (~5500ms = ~550 dispatched, 500 in-flight)
  await new Promise((r) => setTimeout(r, 5500));

  // Inject single counterbids spaced 500ms apart (realistic arrival rate)
  const NUM_COUNTERBIDS = 10;
  for (let i = 0; i < NUM_COUNTERBIDS; i++) {
    const eventTime = performance.now();
    await limiter.schedule({ priority: 0 }, async () => {
      latencies.push(performance.now() - eventTime);
      await simulateLatency(SIMULATED_LATENCY);
    });
    // Wait 500ms between counterbids to simulate realistic arrival
    if (i < NUM_COUNTERBIDS - 1) {
      await new Promise((r) => setTimeout(r, 500));
    }
  }

  await Promise.all(bgPromises);

  latencies.sort((a, b) => a - b);
  const p50 = latencies[Math.floor(latencies.length * 0.5)];
  const p99 = latencies[Math.floor(latencies.length * 0.99)];
  const max = latencies[latencies.length - 1];

  // Under saturated conditions with a single counterbid:
  // - Wait for next slot to free: ~2ms avg (500 slots / 1000ms latency)
  // - Wait for timing interval: 0-10ms
  // - Total: ~12ms avg, p99 < 25ms
  const passed = p99 < 25;

  console.log(`Counterbids: ${latencies.length}`);
  console.log(`Latency: p50=${p50.toFixed(2)}ms | p99=${p99.toFixed(2)}ms | max=${max.toFixed(2)}ms`);
  console.log(`p99 < 25ms: ${passed ? 'PASS' : 'FAIL'}`);

  results.push({
    name: 'D: Single counterbid latency (saturated, p99 < 25ms)',
    passed,
    details: [
      `p50=${p50.toFixed(2)}ms p99=${p99.toFixed(2)}ms max=${max.toFixed(2)}ms`,
      `${BG_COUNT} background jobs, ${MAX_CONCURRENT} slots, ${SIMULATED_LATENCY}ms latency`,
      'Single counterbid injected at a time (realistic arrival pattern)',
    ],
  });

  return passed;
}

// ============================================
// MAIN
// ============================================
async function main() {
  console.log('Counterbid End-to-End Latency Test');
  console.log(`Config: ${RATE_LIMIT} RPS, maxConcurrent=${MAX_CONCURRENT}`);
  console.log('='.repeat(60));

  await testNoLoadLatency();
  await testSteadyLoadLatency();
  await testDedupEffectiveness();
  await testPriorityJumpLatency();

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
  console.log('KEY INSIGHT:');
  console.log('  Direct dispatch path: WebSocket → dedup (<0.1ms) → limiter.schedule (0-10ms) → API call');
  console.log('  vs BullMQ path: WebSocket → Redis write (2-5ms) → Worker pickup (5-20ms) → processJob (2-5ms) → limiter (0-10ms)');
  console.log('='.repeat(60));

  console.log('\n' + (allPassed ? 'ALL TESTS PASSED' : 'SOME TESTS FAILED'));
  process.exit(allPassed ? 0 : 1);
}

main().catch((err) => {
  console.error('Test suite error:', err);
  process.exit(2);
});
