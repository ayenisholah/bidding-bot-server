/**
 * Process Lifecycle Logger
 *
 * Logs process lifecycle events (start, stop, crash, signals) to a dedicated file
 * for diagnosing unexpected restarts and crashes that occur outside Node.js control.
 *
 * This is separate from debug-file-logger.ts because it needs to:
 * 1. Work before and after the main application is fully initialized
 * 2. Log synchronously during crash/exit scenarios
 * 3. Capture low-level process events like signals and uncaught exceptions
 */

import fs from 'fs';
import path from 'path';
import v8 from 'v8';

// Log file path - uses same directory as other debug logs
// In development, use local debug-logs directory; in production, use /app/debug-logs
const getLogDir = () => {
  if (process.env.DEBUG_LOG_DIR) {
    return process.env.DEBUG_LOG_DIR;
  }
  // In production (Docker), use /app/debug-logs
  // In development, use local debug-logs directory relative to project root
  if (process.env.NODE_ENV === 'production') {
    return '/app/debug-logs';
  }
  // Development: use local directory
  return path.join(process.cwd(), 'debug-logs');
};

const LOG_DIR = getLogDir();

const getLogFilePath = () => {
  const date = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
  return path.join(LOG_DIR, `process-lifecycle-${date}.log`);
};

// Memory usage tracking interval (30 seconds for detailed crash debugging)
let memoryMonitorInterval: NodeJS.Timeout | null = null;
const MEMORY_MONITOR_INTERVAL = 30 * 1000;  // Changed from 5 minutes to 30 seconds

// Track previous memory snapshot for delta calculation
let lastMemorySnapshot: {
  heapUsed: number;
  heapTotal: number;
  rss: number;
  external: number;
  arrayBuffers: number;
  timestamp: number;
} | null = null;

// GC frequency tracking (ENHANCED)
interface GCEvent {
  timestamp: number;
  heapFreedMB: number;
  type: 'automatic' | 'proactive'; // NEW: Track GC source
}

const gcEvents: GCEvent[] = [];
const GC_TRACKING_WINDOW_MS = 60 * 60 * 1000;  // 1 hour

// ENHANCED: Separate tracking for automatic vs proactive GC
let automaticGCCount = 0;
let proactiveGCCount = 0;

// OOM Prevention thresholds (2026-02-03 - Proactive GC strategy on 8GB server)
// With proactive GC at 50%, warning/critical thresholds serve as safety nets for GC failure scenarios
const MEMORY_WARNING_MB = 3000;  // 3GB (69% of 4.25GB) - GC may be ineffective
const MEMORY_CRITICAL_MB = 3500; // 3.5GB (80% of 4.25GB) - GC failure, manual intervention needed
const MEMORY_GUARD_INTERVAL = 30 * 1000; // Check every 30 seconds
let memoryGuardInterval: NodeJS.Timeout | null = null;

// Track if we've already warned about write failures to avoid spam
let writeFailureWarned = false;

/**
 * Write a log entry synchronously (important for crash scenarios)
 */
function writeLogSync(level: string, message: string, data?: Record<string, unknown>): void {
  try {
    // Ensure log directory exists
    if (!fs.existsSync(LOG_DIR)) {
      fs.mkdirSync(LOG_DIR, { recursive: true });
    }

    const timestamp = new Date().toISOString();
    const dataStr = data ? ` | ${JSON.stringify(data)}` : '';
    const logLine = `[${timestamp}] [LIFECYCLE] [${level}] ${message}${dataStr}\n`;

    fs.appendFileSync(getLogFilePath(), logLine);
  } catch (error) {
    // Only warn once about write failures to avoid spam
    if (!writeFailureWarned) {
      writeFailureWarned = true;
      // In development, this is expected if debug-logs doesn't exist - just silently skip
      if (process.env.NODE_ENV !== 'production') {
        // Silently ignore in development
        return;
      }
      console.error(`[LIFECYCLE] Failed to write log (will not warn again): ${error}`);
    }
  }
}

/**
 * Get current memory usage in MB
 */
function getMemoryUsage(): Record<string, number> {
  const usage = process.memoryUsage();
  return {
    heapUsed: Math.round(usage.heapUsed / 1024 / 1024),
    heapTotal: Math.round(usage.heapTotal / 1024 / 1024),
    rss: Math.round(usage.rss / 1024 / 1024),
    external: Math.round(usage.external / 1024 / 1024),
    arrayBuffers: Math.round(usage.arrayBuffers / 1024 / 1024),
  };
}

/**
 * Get detailed memory usage with growth rate (delta from last sample)
 * Used for crash debugging - logs every 30 seconds
 */
function getDetailedMemoryUsage(): {
  current: Record<string, number>;
  delta: Record<string, number> | null;
  gcAvailable: boolean;
} {
  const usage = process.memoryUsage();
  const now = Date.now();

  const current = {
    heapUsed: Math.round(usage.heapUsed / 1024 / 1024),
    heapTotal: Math.round(usage.heapTotal / 1024 / 1024),
    rss: Math.round(usage.rss / 1024 / 1024),
    external: Math.round(usage.external / 1024 / 1024),
    arrayBuffers: Math.round(usage.arrayBuffers / 1024 / 1024),
  };

  let delta: Record<string, number> | null = null;

  if (lastMemorySnapshot) {
    const timeDeltaSec = (now - lastMemorySnapshot.timestamp) / 1000;
    delta = {
      heapUsed: current.heapUsed - lastMemorySnapshot.heapUsed,
      heapTotal: current.heapTotal - lastMemorySnapshot.heapTotal,
      rss: current.rss - lastMemorySnapshot.rss,
      external: current.external - lastMemorySnapshot.external,
      arrayBuffers: current.arrayBuffers - lastMemorySnapshot.arrayBuffers,
      timeDeltaSec: Math.round(timeDeltaSec),
    };
  }

  // Update last snapshot
  lastMemorySnapshot = {
    ...current,
    timestamp: now,
  };

  return {
    current,
    delta,
    gcAvailable: typeof global.gc === 'function',
  };
}

/**
 * Get enhanced GC statistics for API consumption
 */
export function getGCStats() {
  const now = Date.now();

  // Remove stale events and update counters
  const cutoff = now - GC_TRACKING_WINDOW_MS;
  while (gcEvents.length > 0 && gcEvents[0].timestamp < cutoff) {
    const removed = gcEvents.shift();
    if (removed?.type === 'proactive') {
      proactiveGCCount = Math.max(0, proactiveGCCount - 1);
    } else {
      automaticGCCount = Math.max(0, automaticGCCount - 1);
    }
  }

  const recentGCs = gcEvents.filter(gc => (now - gc.timestamp) < GC_TRACKING_WINDOW_MS);

  return {
    totalLastHour: recentGCs.length,
    proactiveGCs: proactiveGCCount,
    automaticGCs: automaticGCCount,
    avgIntervalMinutes: recentGCs.length > 1
      ? Math.round((GC_TRACKING_WINDOW_MS / 1000 / 60) / recentGCs.length)
      : 0,
    recentEvents: recentGCs.slice(-5).map(gc => ({
      timestamp: new Date(gc.timestamp).toISOString(),
      freedMB: gc.heapFreedMB,
      type: gc.type
    }))
  };
}

/**
 * Get memory stats for API consumption (does NOT update lastMemorySnapshot)
 * Returns current memory usage, delta from last logged sample, and thresholds
 */
export function getMemoryStats(): {
  current: {
    heapUsed: number;
    heapTotal: number;
    rss: number;
    external: number;
    arrayBuffers: number;
  };
  delta: {
    heapUsed: number;
    heapTotal: number;
    rss: number;
    external: number;
    arrayBuffers: number;
    timeDeltaSec: number;
  } | null;
  thresholds: {
    warningMB: number;
    criticalMB: number;
  };
  status: 'normal' | 'warning' | 'critical';
  gcAvailable: boolean;
  uptime: number;
  pid: number;
} {
  const usage = process.memoryUsage();
  const now = Date.now();

  const current = {
    heapUsed: Math.round(usage.heapUsed / 1024 / 1024),
    heapTotal: Math.round(usage.heapTotal / 1024 / 1024),
    rss: Math.round(usage.rss / 1024 / 1024),
    external: Math.round(usage.external / 1024 / 1024),
    arrayBuffers: Math.round(usage.arrayBuffers / 1024 / 1024),
  };

  let delta: {
    heapUsed: number;
    heapTotal: number;
    rss: number;
    external: number;
    arrayBuffers: number;
    timeDeltaSec: number;
  } | null = null;

  if (lastMemorySnapshot) {
    const timeDeltaSec = (now - lastMemorySnapshot.timestamp) / 1000;
    delta = {
      heapUsed: current.heapUsed - lastMemorySnapshot.heapUsed,
      heapTotal: current.heapTotal - lastMemorySnapshot.heapTotal,
      rss: current.rss - lastMemorySnapshot.rss,
      external: current.external - lastMemorySnapshot.external,
      arrayBuffers: current.arrayBuffers - lastMemorySnapshot.arrayBuffers,
      timeDeltaSec: Math.round(timeDeltaSec),
    };
  }

  // Determine status based on RSS thresholds
  let status: 'normal' | 'warning' | 'critical' = 'normal';
  if (current.rss > MEMORY_CRITICAL_MB) {
    status = 'critical';
  } else if (current.rss > MEMORY_WARNING_MB) {
    status = 'warning';
  }

  return {
    current,
    delta,
    thresholds: {
      warningMB: MEMORY_WARNING_MB,
      criticalMB: MEMORY_CRITICAL_MB,
    },
    status,
    gcAvailable: typeof global.gc === 'function',
    uptime: Math.round(process.uptime()),
    pid: process.pid,
  };
}

/**
 * Log process start with system information
 */
export function logProcessStart(): void {
  const memory = getMemoryUsage();
  const v8Stats = v8.getHeapStatistics();

  writeLogSync('INFO', '🚀 PROCESS STARTED', {
    pid: process.pid,
    nodeVersion: process.version,
    platform: process.platform,
    arch: process.arch,
    memory,
    v8HeapStats: {
      heapSizeLimitMB: Math.round(v8Stats.heap_size_limit / 1024 / 1024),
      totalHeapSizeMB: Math.round(v8Stats.total_heap_size / 1024 / 1024),
      usedHeapSizeMB: Math.round(v8Stats.used_heap_size / 1024 / 1024),
      mallocedMemoryMB: Math.round(v8Stats.malloced_memory / 1024 / 1024),
      peakMallocedMemoryMB: Math.round(v8Stats.peak_malloced_memory / 1024 / 1024),
      doesZapGarbage: v8Stats.does_zap_garbage
    },
    gcAvailable: typeof global.gc === 'function',
    env: process.env.NODE_ENV || 'development',
    uptime: 0,
  });

  // Start memory monitoring
  startMemoryMonitor();

  // Start OOM prevention memory guard (2026-01-15)
  startMemoryGuard();

  // Register signal handlers
  registerSignalHandlers();

  // Register crash handlers
  registerCrashHandlers();
}

/**
 * Log process exit
 */
export function logProcessExit(code: number, reason: string): void {
  const memory = getMemoryUsage();

  writeLogSync('INFO', `⏹️ PROCESS EXITING`, {
    exitCode: code,
    reason,
    pid: process.pid,
    uptime: Math.round(process.uptime()),
    memory,
  });
}

/**
 * Detect and log garbage collection events
 * ENHANCED: Track automatic vs proactive GC
 */
function detectAndLogGC(current: Record<string, number>, delta: Record<string, number> | null): void {
  if (!delta) return;

  const heapDelta = delta.heapUsed;
  const rssDelta = delta.rss;

  // GC detected if significant memory freed (>10MB heap reduction)
  if (heapDelta < -10) {
    const now = Date.now();

    // Determine GC type based on timing
    // Proactive GC runs every 30s, so if last GC was ~30s ago, likely proactive
    const lastGC = gcEvents[gcEvents.length - 1];
    const timeSinceLastGC = lastGC ? now - lastGC.timestamp : Infinity;
    const isProactive = timeSinceLastGC >= 29000 && timeSinceLastGC <= 31000; // 30s ± 1s
    const gcType = isProactive ? 'proactive' : 'automatic';

    if (gcType === 'proactive') {
      proactiveGCCount++;
    } else {
      automaticGCCount++;
    }

    gcEvents.push({
      timestamp: now,
      heapFreedMB: Math.abs(heapDelta),
      type: gcType
    });

    // Keep only last hour
    while (gcEvents.length > 0 && (now - gcEvents[0].timestamp) > GC_TRACKING_WINDOW_MS) {
      const removed = gcEvents.shift();
      if (removed?.type === 'proactive') proactiveGCCount = Math.max(0, proactiveGCCount - 1);
      else automaticGCCount = Math.max(0, automaticGCCount - 1);
    }

    const gcCountLastHour = gcEvents.length;
    const avgGcIntervalMin = gcCountLastHour > 1
      ? Math.round((GC_TRACKING_WINDOW_MS / 1000 / 60) / gcCountLastHour)
      : 0;

    writeLogSync('INFO', `🧹 GC_DETECTED [${gcType.toUpperCase()}]`, {
      heapFreedMB: Math.abs(heapDelta),
      rssFreedMB: Math.round(Math.abs(rssDelta)),
      currentHeapMB: current.heapUsed,
      currentRssMB: current.rss,
      gcCountLastHour,
      proactiveGCs: proactiveGCCount,
      automaticGCs: automaticGCCount,
      avgGcIntervalMin
    });
  }
}

/**
 * Log memory usage (called periodically every 30 seconds)
 * Includes delta from last sample to track memory growth rate
 */
export function logMemoryUsage(): void {
  const { current, delta, gcAvailable } = getDetailedMemoryUsage();

  // Detect GC before logging snapshot
  detectAndLogGC(current, delta);

  writeLogSync('DEBUG', '📊 MEMORY_SNAPSHOT', {
    ...current,
    delta: delta || 'first_sample',
    gcAvailable,
    uptime: Math.round(process.uptime()),
    pid: process.pid,
  });
}

/**
 * Log signal received
 */
export function logSignal(signal: string): void {
  writeLogSync('WARN', `⚡ SIGNAL RECEIVED: ${signal}`, {
    pid: process.pid,
    uptime: Math.round(process.uptime()),
  });
}

/**
 * Log crash/error with detailed memory stats including growth rate
 */
export function logCrash(error: Error, type: 'uncaughtException' | 'unhandledRejection'): void {
  const { current, delta, gcAvailable } = getDetailedMemoryUsage();

  writeLogSync('ERROR', `💥 ${type.toUpperCase()}`, {
    error: error.message,
    stack: error.stack,
    pid: process.pid,
    uptime: Math.round(process.uptime()),
    memory: current,
    memoryDelta: delta || 'first_sample',
    gcAvailable,
  });
}

/**
 * Log custom event
 */
export function logLifecycleEvent(event: string, data?: Record<string, unknown>): void {
  writeLogSync('INFO', event, {
    ...data,
    pid: process.pid,
    uptime: Math.round(process.uptime()),
  });
}

/**
 * Start periodic memory monitoring
 */
function startMemoryMonitor(): void {
  if (memoryMonitorInterval) {
    clearInterval(memoryMonitorInterval);
  }

  memoryMonitorInterval = setInterval(() => {
    logMemoryUsage();
  }, MEMORY_MONITOR_INTERVAL);

  // Don't keep process alive just for memory monitoring
  memoryMonitorInterval.unref();
}

/**
 * Stop memory monitoring (call on shutdown)
 */
export function stopMemoryMonitor(): void {
  if (memoryMonitorInterval) {
    clearInterval(memoryMonitorInterval);
    memoryMonitorInterval = null;
  }
}

/**
 * Start proactive memory guard to prevent OOM kills (2026-01-15)
 * Monitors RSS and triggers GC when approaching critical thresholds.
 * Requires Node.js started with --expose-gc flag for global.gc() to work.
 */
function startMemoryGuard(): void {
  if (memoryGuardInterval) {
    clearInterval(memoryGuardInterval);
  }

  memoryGuardInterval = setInterval(() => {
    const usage = process.memoryUsage();
    const rssMB = Math.round(usage.rss / 1024 / 1024);
    const heapUsedMB = Math.round(usage.heapUsed / 1024 / 1024);

    if (rssMB > MEMORY_CRITICAL_MB) {
      // CRITICAL - Log and force GC if available
      writeLogSync('ERROR', '🚨 MEMORY_CRITICAL', {
        rss: rssMB,
        heapUsed: heapUsedMB,
        threshold: MEMORY_CRITICAL_MB,
        action: 'attempting_gc',
      });

      // Force garbage collection if available (requires --expose-gc flag)
      if (typeof global.gc === 'function') {
        const rssBefore = rssMB;
        const heapBefore = heapUsedMB;
        global.gc();
        const usageAfter = process.memoryUsage();
        const rssAfter = Math.round(usageAfter.rss / 1024 / 1024);
        const heapAfter = Math.round(usageAfter.heapUsed / 1024 / 1024);
        writeLogSync('INFO', '🧹 FORCED_GC_COMPLETE', {
          rssBefore,
          rssAfter,
          rssFreed: rssBefore - rssAfter,
          heapBefore,
          heapAfter,
          heapFreed: heapBefore - heapAfter,
        });
      } else {
        writeLogSync('WARN', '⚠️ GC_NOT_AVAILABLE', {
          message: 'global.gc() not available - start Node.js with --expose-gc flag',
        });
      }
    } else if (rssMB > MEMORY_WARNING_MB) {
      // WARNING - Log but don't take action yet
      writeLogSync('WARN', '⚠️ MEMORY_WARNING', {
        rss: rssMB,
        heapUsed: heapUsedMB,
        threshold: MEMORY_WARNING_MB,
        criticalThreshold: MEMORY_CRITICAL_MB,
      });
    }
  }, MEMORY_GUARD_INTERVAL);

  // Don't keep process alive just for memory guard
  memoryGuardInterval.unref();
}

/**
 * Stop memory guard (call on shutdown)
 */
export function stopMemoryGuard(): void {
  if (memoryGuardInterval) {
    clearInterval(memoryGuardInterval);
    memoryGuardInterval = null;
  }
}

/**
 * Register handlers for process signals
 */
function registerSignalHandlers(): void {
  // SIGINT (Ctrl+C)
  process.on('SIGINT', () => {
    logSignal('SIGINT');
  });

  // SIGTERM (graceful termination request)
  process.on('SIGTERM', () => {
    logSignal('SIGTERM');
  });

  // SIGHUP (terminal hangup)
  process.on('SIGHUP', () => {
    logSignal('SIGHUP');
  });

  // SIGUSR1 (user-defined signal, often used for debugging)
  process.on('SIGUSR1', () => {
    logSignal('SIGUSR1');
    logMemoryUsage(); // Log memory on USR1 for debugging
  });

  // SIGUSR2 (user-defined signal)
  process.on('SIGUSR2', () => {
    logSignal('SIGUSR2');
  });

  // beforeExit - emitted when Node.js empties event loop
  process.on('beforeExit', (code) => {
    writeLogSync('INFO', `🔄 BEFORE EXIT (event loop empty)`, {
      exitCode: code,
      pid: process.pid,
      uptime: Math.round(process.uptime()),
    });
  });

  // exit - emitted when process is about to exit
  process.on('exit', (code) => {
    stopMemoryMonitor();
    stopMemoryGuard();
    writeLogSync('INFO', `🛑 PROCESS EXIT`, {
      exitCode: code,
      pid: process.pid,
      uptime: Math.round(process.uptime()),
    });
  });
}

/**
 * Register handlers for crashes
 */
function registerCrashHandlers(): void {
  // Uncaught exceptions
  process.on('uncaughtException', (error) => {
    logCrash(error, 'uncaughtException');
    // Don't exit here - let the main app handle it
  });

  // Unhandled promise rejections
  process.on('unhandledRejection', (reason) => {
    const error = reason instanceof Error ? reason : new Error(String(reason));
    logCrash(error, 'unhandledRejection');
    // Don't exit here - let the main app handle it
  });

  // Warning events
  process.on('warning', (warning) => {
    writeLogSync('WARN', `⚠️ NODE WARNING: ${warning.name}`, {
      message: warning.message,
      stack: warning.stack,
    });
  });
}
