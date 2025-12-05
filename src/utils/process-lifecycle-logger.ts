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

// Memory usage tracking interval (5 minutes)
let memoryMonitorInterval: NodeJS.Timeout | null = null;
const MEMORY_MONITOR_INTERVAL = 5 * 60 * 1000;

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
  };
}

/**
 * Log process start with system information
 */
export function logProcessStart(): void {
  const memory = getMemoryUsage();

  writeLogSync('INFO', '🚀 PROCESS STARTED', {
    pid: process.pid,
    nodeVersion: process.version,
    platform: process.platform,
    arch: process.arch,
    memory,
    env: process.env.NODE_ENV || 'development',
    uptime: 0,
  });

  // Start memory monitoring
  startMemoryMonitor();

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
 * Log memory usage (called periodically)
 */
export function logMemoryUsage(): void {
  const memory = getMemoryUsage();

  writeLogSync('DEBUG', '📊 MEMORY USAGE', {
    ...memory,
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
 * Log crash/error
 */
export function logCrash(error: Error, type: 'uncaughtException' | 'unhandledRejection'): void {
  const memory = getMemoryUsage();

  writeLogSync('ERROR', `💥 ${type.toUpperCase()}`, {
    error: error.message,
    stack: error.stack,
    pid: process.pid,
    uptime: Math.round(process.uptime()),
    memory,
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
