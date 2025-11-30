import * as fs from 'fs';
import * as path from 'path';

const DEBUG = process.env.DEBUG === 'true';
const LOG_DIR = path.resolve(process.cwd(), 'debug-logs');

// Ensure directory exists on module load
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

// ============================================
// LOG CATEGORY DEFINITIONS (22 categories)
// ============================================

type LogCategory =
  | 'startup'
  | 'task'
  | 'scheduler'
  | 'bidflow'
  | 'counterbid'
  | 'queue'
  | 'worker'
  | 'database'
  | 'redis'
  | 'lock'
  | 'blur'
  | 'opensea'
  | 'magiceden'
  | 'api'
  | 'websocket'
  | 'listing'
  | 'collection'
  | 'balance'
  | 'cancellation'
  | 'health'
  | 'metadata'
  | 'system';

// Prefix → Category mapping (single point of configuration)
const PREFIX_TO_CATEGORY: Record<string, LogCategory> = {
  // Startup (5)
  'STARTUP': 'startup',
  'Startup': 'startup',
  'ServerStartup': 'startup',
  'ServerRestart': 'startup',
  'BullMQInit': 'startup',

  // Task (7)
  'Task': 'task',
  'TaskStart': 'task',
  'TaskStatusCheck': 'task',
  'TaskStatusTransition': 'task',
  'TaskStatus': 'task',
  'TaskStateUpdate': 'task',
  'TaskRecovery': 'task',

  // Scheduler (3)
  'TaskScheduler': 'scheduler',
  'Scheduler': 'scheduler',
  'TimedLockManager': 'scheduler',

  // BidFlow (5)
  'BidFlow': 'bidflow',
  'ScheduledBid': 'bidflow',
  'Preprocessing': 'bidflow',
  'PriceCalc': 'bidflow',
  'StopOption': 'bidflow',

  // Counterbid (1)
  'Counterbid': 'counterbid',

  // Queue (12)
  'Queue': 'queue',
  'BlurQueue': 'queue',
  'BlurQueueEvents': 'queue',
  'QueueClear': 'queue',
  'QueuePause': 'queue',
  'QueueResume': 'queue',
  'QueueEvent': 'queue',
  'QueueDrainMonitor': 'queue',
  'JobProcessing': 'queue',
  'JobRetry': 'queue',
  'Performance': 'queue',
  'BulkJobs': 'queue',

  // Worker (6)
  'Worker': 'worker',
  'BlurWorker': 'worker',
  'WorkerHealth': 'worker',
  'WorkerHealthCheck': 'worker',
  'WorkerFailure': 'worker',
  'WorkerResume': 'worker',

  // Database (6)
  'Database': 'database',
  'MongoDBConnection': 'database',
  'MongoDBPool': 'database',
  'MongoDBPoolMonitoring': 'database',
  'ConnectionPool': 'database',
  'DB Retry': 'database',

  // Redis (3)
  'Redis': 'redis',
  'RedisOperation': 'redis',
  'RedisRecovery': 'redis',

  // Lock (2)
  'Lock': 'lock',
  'LockRelease': 'lock',

  // Marketplaces (3)
  'Blur': 'blur',
  'OpenSea': 'opensea',
  'MagicEden': 'magiceden',

  // API (2)
  'API': 'api',
  'Blockchain': 'api',

  // WebSocket (1)
  'WebSocket': 'websocket',

  // Listing (5)
  'Listing': 'listing',
  'ListingFetch': 'listing',
  'ListingRestart': 'listing',
  'ListingPriceCache': 'listing',
  'ListingWebSocket': 'listing',
  'ListingScheduler': 'listing',

  // Collection (3)
  'Collection': 'collection',
  'TRAIT FLOOR': 'collection',
  'CollectionStats': 'collection',

  // Balance (1)
  'Balance': 'balance',

  // Cancellation (7)
  'Cancel': 'cancellation',
  'CancellationTracking': 'cancellation',
  'OrderCleanup': 'cancellation',
  'BulkCancelOpensea': 'cancellation',
  'BulkCancelMagiceden': 'cancellation',
  'ValidateBidCount': 'cancellation',
  'TopBids': 'cancellation',

  // Health (3)
  'Health': 'health',
  'HealthCheck': 'health',
  'ConnectivityMonitor': 'health',

  // Metadata (2)
  'Metadata': 'metadata',
  'Rarity': 'metadata',

  // System (explicit - others fallback here)
  'General': 'system',
  'Logger': 'system',
  'Shutdown': 'system',
  'Security': 'system',
  'Authentication': 'system',
  'PasswordCheck': 'system',
  'Crypto': 'system',
  'Cache': 'system',
  'Validation': 'system',
  'UnhandledRejection': 'system',
  'UncaughtException': 'system',
  'TaskState': 'task',
};

// ============================================
// HELPER FUNCTIONS
// ============================================

function getCategoryForPrefix(prefix: string): LogCategory {
  return PREFIX_TO_CATEGORY[prefix] || 'system';
}

function getLogFilePath(prefix: string): string {
  const date = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
  const category = getCategoryForPrefix(prefix);
  return path.join(LOG_DIR, `${category}-${date}.log`);
}

function writeToFile(logLine: string, prefix: string): void {
  try {
    fs.appendFileSync(getLogFilePath(prefix), logLine + '\n');
  } catch {
    // Silently fail file write to not break app
  }
}

// Format error for logging
function formatError(error: unknown): string {
  if (error instanceof Error) {
    return `${error.message}${error.stack ? `\n${error.stack}` : ''}`;
  }
  if (typeof error === 'string') {
    return error;
  }
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
}

// ============================================
// PUBLIC API (unchanged signatures)
// ============================================

/**
 * Log a debug message to both console and file.
 * Only logs when DEBUG=true environment variable is set.
 * Routes to category-specific log file based on prefix.
 *
 * @param prefix - Log prefix category (e.g., 'TaskScheduler', 'BidFlow', 'STARTUP')
 * @param message - Log message content
 */
export function debugLog(prefix: string, message: string): void {
  if (!DEBUG) return;

  const timestamp = new Date().toISOString();
  const logLine = `[${timestamp}] [${prefix}] ${message}`;

  // Console output
  console.log(logLine);

  // File output (append to category-specific file)
  writeToFile(logLine, prefix);
}

/**
 * Log an error message to both console and file.
 * ALWAYS logs regardless of DEBUG flag (errors are always important).
 * Routes to category-specific log file based on prefix.
 *
 * @param prefix - Log prefix category (e.g., 'API', 'Database', 'Queue')
 * @param message - Error context message
 * @param error - Optional error object or value
 */
export function errorLog(prefix: string, message: string, error?: unknown): void {
  const timestamp = new Date().toISOString();
  const errorDetails = error ? ` | ${formatError(error)}` : '';
  const logLine = `[${timestamp}] [ERROR][${prefix}] ${message}${errorDetails}`;

  // Console output (use console.error for errors)
  console.error(logLine);

  // File output (append to category-specific file)
  writeToFile(logLine, prefix);
}

/**
 * Log a warning message to both console and file.
 * ALWAYS logs regardless of DEBUG flag (warnings are always important).
 * Routes to category-specific log file based on prefix.
 *
 * @param prefix - Log prefix category (e.g., 'Balance', 'Cache', 'WebSocket')
 * @param message - Warning message content
 */
export function warnLog(prefix: string, message: string): void {
  const timestamp = new Date().toISOString();
  const logLine = `[${timestamp}] [WARN][${prefix}] ${message}`;

  // Console output (use console.warn for warnings)
  console.warn(logLine);

  // File output (append to category-specific file)
  writeToFile(logLine, prefix);
}

/**
 * Log an info message to both console and file.
 * ALWAYS logs regardless of DEBUG flag (for important status messages).
 * Routes to category-specific log file based on prefix.
 *
 * @param prefix - Log prefix category (e.g., 'WebSocket', 'STARTUP')
 * @param message - Info message content
 */
export function infoLog(prefix: string, message: string): void {
  const timestamp = new Date().toISOString();
  const logLine = `[${timestamp}] [INFO][${prefix}] ${message}`;

  // Console output
  console.log(logLine);

  // File output (append to category-specific file)
  writeToFile(logLine, prefix);
}
