import { LatencySnapshot, ILatencySnapshot, LatencyStats, ExecutionStats } from '../models/latency-snapshot.model';
import { getForwardingStats } from './forwarding-latency-tracker';
import { getAllExecutionStats } from './execution-latency-tracker';
import { errorLog, warnLog } from '../utils/debug-file-logger';

/**
 * Interval handle for periodic snapshots
 */
let snapshotInterval: NodeJS.Timeout | null = null;

/**
 * Snapshot frequency (5 minutes)
 */
const SNAPSHOT_INTERVAL_MS = 5 * 60 * 1000;

/**
 * Helper to create empty LatencyStats object
 */
function createEmptyLatencyStats(): LatencyStats {
  return {
    p50: 0,
    p95: 0,
    p99: 0,
    avg: 0,
    max: 0,
    sampleCount: 0,
  };
}

/**
 * Helper to create empty ExecutionStats object
 */
function createEmptyExecutionStats(): ExecutionStats {
  return {
    p50: 0,
    p95: 0,
    p99: 0,
    avg: 0,
    max: 0,
    sampleCount: 0,
    successRate: 0,
  };
}

/**
 * Capture current latency stats from both trackers + system context
 */
export async function captureLatencySnapshot(): Promise<ILatencySnapshot | null> {
  const startTime = Date.now();

  try {
    // Get forwarding latency stats from tracker
    const forwardingStatsOverall = getForwardingStats();
    const forwardingStatsOpensea = getForwardingStats('opensea');
    const forwardingStatsBlur = getForwardingStats('blur');
    const forwardingStatsMagiceden = getForwardingStats('magiceden');

    // Get execution latency stats from tracker
    const executionStatsAll = getAllExecutionStats();

    // Get system context
    const memUsage = process.memoryUsage();
    const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
    const heapTotalMB = Math.round(memUsage.heapTotal / 1024 / 1024);
    const heapPercent = Math.round((memUsage.heapUsed / memUsage.heapTotal) * 100);
    const rssMB = Math.round(memUsage.rss / 1024 / 1024);

    // Queue stats placeholder (will be populated from index.ts if needed)
    let queueDepth = 0;
    let activeJobs = 0;

    // Note: Queue stats are optional - index.ts can call this function
    // with queue data passed through global state if needed

    // Build snapshot document
    const snapshot = new LatencySnapshot({
      timestamp: new Date(),

      forwardingLatency: {
        overall: forwardingStatsOverall || createEmptyLatencyStats(),
        byMarketplace: {
          opensea: forwardingStatsOpensea || createEmptyLatencyStats(),
          blur: forwardingStatsBlur || createEmptyLatencyStats(),
          magiceden: forwardingStatsMagiceden || createEmptyLatencyStats(),
        },
      },

      executionLatency: {
        overall: executionStatsAll.overall || createEmptyExecutionStats(),
        byMarketplace: {
          opensea: executionStatsAll.byMarketplace.opensea || createEmptyExecutionStats(),
          blur: executionStatsAll.byMarketplace.blur || createEmptyExecutionStats(),
          magiceden: executionStatsAll.byMarketplace.magiceden || createEmptyExecutionStats(),
        },
      },

      context: {
        heapUsedMB,
        heapPercent,
        rssMB,
        queueDepth: {
          main: queueDepth,
          counterbid: 0, // No separate counterbid queue
        },
        activeJobs: {
          main: activeJobs,
          counterbid: 0, // No separate counterbid queue
        },
      },

      metadata: {
        snapshotDurationMs: Date.now() - startTime,
      },
    });

    // Save to MongoDB
    const savedSnapshot = await snapshot.save();

    const duration = Date.now() - startTime;


    return savedSnapshot;
  } catch (error) {
    errorLog('LatencySnapshot', 'Failed to capture snapshot', {
      error: String(error),
      duration: `${Date.now() - startTime}ms`,
    });
    return null;
  }
}

/**
 * Periodic snapshot creation (called every 5 minutes)
 */
export async function startPeriodicSnapshots(): Promise<void> {
  // Don't start if already running
  if (snapshotInterval) {
    warnLog('LatencySnapshot', 'Periodic snapshots already running');
    return;
  }


  // Capture initial snapshot immediately
  await captureLatencySnapshot();

  // Start periodic snapshots
  snapshotInterval = setInterval(async () => {
    await captureLatencySnapshot();
  }, SNAPSHOT_INTERVAL_MS);
}

/**
 * Graceful shutdown
 */
export async function stopPeriodicSnapshots(): Promise<void> {
  if (snapshotInterval) {
    clearInterval(snapshotInterval);
    snapshotInterval = null;

    // Capture final snapshot before shutdown
    await captureLatencySnapshot();

  }
}
