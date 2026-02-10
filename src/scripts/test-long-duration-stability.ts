/**
 * Long-Duration Stability Test
 *
 * 60 seconds under production-like conditions:
 * - 1000ms average latency
 * - 5% error rate with 2s retry delay
 * - Mixed priorities (90% p5, 10% p0)
 *
 * Asserts:
 * - Average RPS >= 98
 * - No 5-second window below 90 RPS
 *
 * Run with: npx ts-node src/scripts/test-long-duration-stability.ts
 */
import { PreciseRateLimiter } from '../utils/precise-rate-limiter';

const RATE_LIMIT = 100;
const MIN_TIME = 1000 / RATE_LIMIT; // 10ms
const MAX_CONCURRENT = Math.ceil(RATE_LIMIT * 5); // 500
const TEST_DURATION_SEC = 60;
const ERROR_RATE = 0.05;
const RETRY_DELAY = 2000;
const SIMULATED_LATENCY = 1000;

function simulateLatency(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  console.log('Long-Duration Stability Test (PreciseRateLimiter)');
  console.log(`Config: ${RATE_LIMIT} RPS, ${TEST_DURATION_SEC}s, ${ERROR_RATE * 100}% errors, ${SIMULATED_LATENCY}ms latency`);
  console.log('='.repeat(60));

  const limiter = new PreciseRateLimiter({
    minTime: MIN_TIME,
    maxConcurrent: MAX_CONCURRENT,
    trackDoneStatus: true,
  });

  limiter.on('failed', async (_error, jobInfo) => {
    if (jobInfo.retryCount >= 2) return;
    return RETRY_DELAY;
  });

  const dispatchTimestamps: number[] = [];
  let totalScheduled = 0;
  let failCount = 0;
  const startTime = performance.now();
  const endTime = startTime + TEST_DURATION_SEC * 1000;

  // Continuously schedule jobs for the test duration
  const pendingPromises: Promise<void>[] = [];

  // Schedule jobs at ~120 RPS to keep the queue fed (slightly above target)
  const SCHEDULE_INTERVAL = 8; // ~125 jobs/sec schedule rate
  let lastLogSec = 0;

  while (performance.now() < endTime) {
    const priority = Math.random() < 0.10 ? 0 : 5; // 10% counterbids
    totalScheduled++;

    const p = limiter.schedule({ priority }, async () => {
      dispatchTimestamps.push(performance.now());
      if (Math.random() < ERROR_RATE) {
        failCount++;
        throw new Error('Simulated failure');
      }
      await simulateLatency(SIMULATED_LATENCY + (Math.random() - 0.5) * 500); // 750-1250ms
    }).catch(() => {}); // Suppress unhandled rejections from retries that exhaust

    pendingPromises.push(p);

    // Wait before scheduling next
    await new Promise((r) => setTimeout(r, SCHEDULE_INTERVAL));

    // Log progress every 10 seconds
    const elapsedSec = Math.floor((performance.now() - startTime) / 1000);
    if (elapsedSec > lastLogSec && elapsedSec % 10 === 0) {
      lastLogSec = elapsedSec;
      const counts = limiter.counts();
      const currentDispatches = dispatchTimestamps.length;
      const currentRPS = elapsedSec > 0 ? currentDispatches / elapsedSec : 0;
      console.log(`[${elapsedSec}s] Dispatched: ${currentDispatches} | Avg RPS: ${currentRPS.toFixed(1)} | Queue: ${counts.QUEUED} | Executing: ${counts.EXECUTING} | Failures: ${failCount}`);
    }
  }

  // Record when scheduling stopped to exclude drain tail from analysis
  const schedulingEndTime = performance.now();

  // Wait for all pending jobs to complete
  console.log('\nWaiting for pending jobs to complete...');
  await Promise.all(pendingPromises);

  // Analyze results - only measure dispatches during the active scheduling window
  dispatchTimestamps.sort((a, b) => a - b);

  // Filter timestamps to only the active scheduling period (exclude drain tail)
  const activeTimestamps = dispatchTimestamps.filter((t) => t <= schedulingEndTime);

  // Overall RPS (active period only)
  const totalDuration = (activeTimestamps[activeTimestamps.length - 1] - activeTimestamps[0]) / 1000;
  const avgRPS = (activeTimestamps.length - 1) / totalDuration;

  // 5-second window analysis (active period only, exclude last 5s near boundary)
  const WINDOW_SIZE = 5000; // 5 seconds
  let minWindowRPS = Infinity;
  let minWindowStart = 0;
  const windowResults: { startSec: number; rps: number }[] = [];
  const analysisEnd = activeTimestamps[activeTimestamps.length - 1] - WINDOW_SIZE;

  for (let windowStart = activeTimestamps[0]; windowStart <= analysisEnd; windowStart += 1000) {
    const windowEnd = windowStart + WINDOW_SIZE;
    const inWindow = activeTimestamps.filter((t) => t >= windowStart && t < windowEnd);
    const windowRPS = inWindow.length / (WINDOW_SIZE / 1000);

    windowResults.push({
      startSec: Math.round((windowStart - activeTimestamps[0]) / 1000),
      rps: windowRPS,
    });

    if (windowRPS < minWindowRPS) {
      minWindowRPS = windowRPS;
      minWindowStart = windowStart;
    }
  }

  // Dispatch spacing analysis (active period only)
  const spacings: number[] = [];
  for (let i = 1; i < activeTimestamps.length; i++) {
    spacings.push(activeTimestamps[i] - activeTimestamps[i - 1]);
  }
  spacings.sort((a, b) => a - b);
  const p50 = spacings[Math.floor(spacings.length * 0.5)];
  const p95 = spacings[Math.floor(spacings.length * 0.95)];
  const p99 = spacings[Math.floor(spacings.length * 0.99)];

  // Results
  console.log('\n' + '='.repeat(60));
  console.log('RESULTS (active scheduling period only)');
  console.log('='.repeat(60));
  console.log(`Active duration: ${totalDuration.toFixed(1)}s`);
  console.log(`Active dispatches: ${activeTimestamps.length} (total including drain: ${dispatchTimestamps.length})`);
  console.log(`Average RPS: ${avgRPS.toFixed(1)} (target >= 98)`);
  console.log(`Min 5s window RPS: ${minWindowRPS.toFixed(1)} (target >= 90)`);
  console.log(`Dispatch spacing: p50=${p50.toFixed(3)}ms | p95=${p95.toFixed(3)}ms | p99=${p99.toFixed(3)}ms`);
  console.log(`Failures: ${failCount} (${((failCount / activeTimestamps.length) * 100).toFixed(1)}%)`);
  console.log(`Total scheduled: ${totalScheduled}`);

  // Show worst 5-second windows
  const worstWindows = [...windowResults].sort((a, b) => a.rps - b.rps).slice(0, 5);
  console.log('\nWorst 5-second windows:');
  for (const w of worstWindows) {
    console.log(`  ${w.startSec}s: ${w.rps.toFixed(1)} RPS`);
  }

  // Acceptance criteria
  const avgRPSOk = avgRPS >= 98;
  const minWindowOk = minWindowRPS >= 90;
  const spacingOk = p50 >= 9.0 && p50 <= 11.0;
  const passed = avgRPSOk && minWindowOk;

  console.log('\n' + '='.repeat(60));
  console.log('ACCEPTANCE CRITERIA');
  console.log('='.repeat(60));
  console.log(`[${avgRPSOk ? 'PASS' : 'FAIL'}] Average RPS >= 98: ${avgRPS.toFixed(1)}`);
  console.log(`[${minWindowOk ? 'PASS' : 'FAIL'}] Min 5s window >= 90 RPS: ${minWindowRPS.toFixed(1)}`);
  console.log(`[${spacingOk ? 'PASS' : 'FAIL'}] Spacing p50 ~10ms: ${p50.toFixed(3)}ms`);

  console.log('\n' + (passed ? 'ALL TESTS PASSED' : 'SOME TESTS FAILED'));
  process.exit(passed ? 0 : 1);
}

main().catch((err) => {
  console.error('Test suite error:', err);
  process.exit(2);
});
