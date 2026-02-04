import v8 from 'v8';

interface GCTriggerStats {
  triggeredAt: number;
  heapUsedMB: number;
  heapLimitMB: number;
  usedPercent: number;
  freedMB?: number;
}

const gcHistory: GCTriggerStats[] = [];
const GC_HISTORY_MAX = 100; // Keep last 100 GC triggers
const GC_TRIGGER_THRESHOLD = 50; // Trigger at 50% heap usage
const GC_CHECK_INTERVAL = 30000; // Check every 30 seconds

let gcCheckInterval: NodeJS.Timeout | null = null;

/**
 * Check heap usage and trigger GC if threshold exceeded
 */
export function checkAndTriggerProactiveGC(): void {
  if (!global.gc) {
    console.warn('[ProactiveGC] global.gc() not available. Start with --expose-gc flag.');
    return;
  }

  const heapStats = v8.getHeapStatistics();
  const heapUsedMB = Math.round(heapStats.used_heap_size / 1024 / 1024);
  const heapLimitMB = Math.round(heapStats.heap_size_limit / 1024 / 1024);
  const usedPercent = (heapStats.used_heap_size / heapStats.heap_size_limit) * 100;

  if (usedPercent >= GC_TRIGGER_THRESHOLD) {
    const beforeUsedMB = heapUsedMB;

    console.log(`[ProactiveGC] TRIGGER | Heap: ${heapUsedMB}MB / ${heapLimitMB}MB (${usedPercent.toFixed(1)}%)`);

    // Trigger full garbage collection
    global.gc();

    // Measure freed memory
    const afterStats = v8.getHeapStatistics();
    const afterUsedMB = Math.round(afterStats.used_heap_size / 1024 / 1024);
    const freedMB = beforeUsedMB - afterUsedMB;

    console.log(`[ProactiveGC] COMPLETED | Freed: ${freedMB}MB | After: ${afterUsedMB}MB / ${heapLimitMB}MB (${((afterStats.used_heap_size / afterStats.heap_size_limit) * 100).toFixed(1)}%)`);

    // Record in history
    gcHistory.push({
      triggeredAt: Date.now(),
      heapUsedMB: beforeUsedMB,
      heapLimitMB,
      usedPercent,
      freedMB
    });

    // Keep history size manageable
    if (gcHistory.length > GC_HISTORY_MAX) {
      gcHistory.shift();
    }
  }
}

/**
 * Start proactive GC monitoring
 */
export function startProactiveGC(): void {
  if (gcCheckInterval) {
    console.warn('[ProactiveGC] Already running');
    return;
  }

  console.log(`[ProactiveGC] Started | Threshold: ${GC_TRIGGER_THRESHOLD}% | Interval: ${GC_CHECK_INTERVAL / 1000}s`);

  gcCheckInterval = setInterval(checkAndTriggerProactiveGC, GC_CHECK_INTERVAL);

  // Don't prevent process exit
  if (gcCheckInterval.unref) {
    gcCheckInterval.unref();
  }
}

/**
 * Stop proactive GC monitoring
 */
export function stopProactiveGC(): void {
  if (gcCheckInterval) {
    clearInterval(gcCheckInterval);
    gcCheckInterval = null;
    console.log('[ProactiveGC] Stopped');
  }
}

/**
 * Get GC trigger statistics
 */
export function getProactiveGCStats() {
  const now = Date.now();
  const last24h = gcHistory.filter(gc => (now - gc.triggeredAt) < 24 * 60 * 60 * 1000);
  const lastHour = gcHistory.filter(gc => (now - gc.triggeredAt) < 60 * 60 * 1000);

  const avgFreedMB = last24h.length > 0
    ? last24h.reduce((sum, gc) => sum + (gc.freedMB || 0), 0) / last24h.length
    : 0;

  return {
    total: gcHistory.length,
    last24Hours: last24h.length,
    lastHour: lastHour.length,
    averageFreedMB: Math.round(avgFreedMB),
    history: gcHistory.slice(-10) // Last 10 triggers
  };
}
