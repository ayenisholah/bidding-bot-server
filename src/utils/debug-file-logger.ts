import * as fs from 'fs';
import * as path from 'path';

const DEBUG = process.env.DEBUG === 'true';
export const LOG_DIR = path.resolve(process.cwd(), 'debug-logs');

// Ensure directory exists on module load and verify write permissions
let CAN_WRITE_LOGS = true;
try {
  if (!fs.existsSync(LOG_DIR)) {
    fs.mkdirSync(LOG_DIR, { recursive: true, mode: 0o777 });
  }
  // Test write permissions by creating and deleting a test file
  const testFile = path.join(LOG_DIR, '.write-test');
  fs.writeFileSync(testFile, 'test');
  fs.unlinkSync(testFile);
} catch (err) {
  CAN_WRITE_LOGS = false;
  console.error(`[LogWriter] CRITICAL: Cannot write to ${LOG_DIR}: ${(err as Error).message}`);
  console.error('[LogWriter] Debug logs will NOT be saved to files! Fix volume permissions.');
}

// ============================================
// ASYNC BATCHED LOG WRITER
// ============================================

/**
 * Buffer for async batched log writes
 * Groups log lines by file path to minimize I/O operations
 */
const logBuffer = new Map<string, string[]>();
const FLUSH_INTERVAL_MS = 1000; // Flush every 1 second
let flushIntervalId: NodeJS.Timeout | null = null;
let isShuttingDown = false;

/**
 * Flush all buffered log lines to their respective files
 * Uses async fs.appendFile to avoid blocking the event loop
 */
function flushLogBuffers(): void {
  if (logBuffer.size === 0) return;

  for (const [filePath, lines] of logBuffer) {
    if (lines.length === 0) continue;

    const content = lines.join('\n') + '\n';

    // Use async append - fire and forget for performance
    fs.appendFile(filePath, content, (err) => {
      if (err && !isShuttingDown) {
        // Only log to console to avoid recursion
        console.error(`[LogWriter] Failed to write to ${filePath}: ${err.message}`);
      }
    });
  }

  // Clear all buffers
  logBuffer.clear();
}

/**
 * Synchronous flush for shutdown scenarios
 * Blocks to ensure all logs are written before process exit
 */
function flushLogBuffersSync(): void {
  if (logBuffer.size === 0) return;

  for (const [filePath, lines] of logBuffer) {
    if (lines.length === 0) continue;

    try {
      fs.appendFileSync(filePath, lines.join('\n') + '\n');
    } catch {
      // Silently fail on shutdown
    }
  }
  logBuffer.clear();
}

// Start the flush interval
flushIntervalId = setInterval(flushLogBuffers, FLUSH_INTERVAL_MS);

// Ensure logs are flushed on process exit
process.on('beforeExit', () => {
  isShuttingDown = true;
  if (flushIntervalId) {
    clearInterval(flushIntervalId);
    flushIntervalId = null;
  }
  flushLogBuffersSync();
});

process.on('SIGINT', () => {
  isShuttingDown = true;
  flushLogBuffersSync();
});

process.on('SIGTERM', () => {
  isShuttingDown = true;
  flushLogBuffersSync();
});

// ============================================
// LOG METADATA INTERFACE
// ============================================

/**
 * Structured metadata for filterable logs.
 * All fields are optional - include what's relevant to the log context.
 */
export interface LogMetadata {
  taskId?: string;
  marketplace?: 'blur' | 'opensea' | 'magiceden' | 'system';
  slug?: string;
  tokenId?: string | number;
  traitKey?: string;
  jobId?: string;
  orderHash?: string;
  wallet?: string;
  identifier?: string;
  [key: string]: unknown; // Extensible for additional context
}

/**
 * Parsed log entry structure for export/filtering.
 */
export interface ParsedLogLine {
  timestamp: string;
  level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR';
  prefix: string;
  category: LogCategory;
  message: string;
  metadata?: LogMetadata;
  raw: string;
}

// ============================================
// LOG CATEGORY DEFINITIONS (24 categories)
// ============================================

export type LogCategory =
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
  | 'stoptask'
  | 'taskflow'  // Consolidated task lifecycle logging - ALL state transitions in one file
  | 'system'
  | 'wsConnection';  // Dedicated log for _doConnect() WebSocket connection errors

// Prefix → Category mapping (single point of configuration)
export const PREFIX_TO_CATEGORY: Record<string, LogCategory> = {
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
  'TaskRecovery': 'taskflow',  // Moved to taskflow for consolidated lifecycle logging

  // Scheduler (3)
  'TaskScheduler': 'scheduler',
  'Scheduler': 'scheduler',
  'TimedLockManager': 'scheduler',

  // BidFlow (7)
  'BidFlow': 'bidflow',
  'ScheduledBid': 'bidflow',
  'Preprocessing': 'bidflow',
  'TokenFetch': 'bidflow',    // Token fetching from OpenSea collection items
  'TokenTrack': 'bidflow',    // Token source tracking and deduplication in preprocessing
  'PriceCalc': 'bidflow',
  'StopOption': 'bidflow',

  // Counterbid (2)
  'Counterbid': 'counterbid',
  'BidCounter': 'counterbid',   // Track own bid counting for message loss debugging

  // Queue (13)
  'Queue': 'queue',
  'BlurQueue': 'queue',
  'BlurQueueEvents': 'queue',
  'QueueClear': 'queue',
  'QueuePause': 'queue',
  'QueueResume': 'queue',
  'QueueEvent': 'taskflow',      // Moved to taskflow for consolidated lifecycle logging
  'QueueDrainMonitor': 'queue',
  'JobProcessing': 'taskflow',   // Moved to taskflow for consolidated lifecycle logging
  'JobRetry': 'queue',
  'Performance': 'queue',
  'BulkJobs': 'queue',
  'StuckJobCleanup': 'queue',

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

  // WebSocket (8)
  'WebSocket': 'websocket',
  'MsgLoss': 'websocket',    // Message loss detection between ws-proxy and server
  'MsgFlow': 'websocket',    // Raw message flow tracking at earliest checkpoint
  'WsProxyRecv': 'websocket',     // Messages received from ws-proxy (detailed tracking)
  'WsProxyDrop': 'websocket',     // Potential drops detected during processing
  'WsProxyLag': 'websocket',      // Latency/lag detection (message timestamp vs receive time)
  'WsProxyReconnect': 'websocket', // ws-proxy reconnection events
  'WsProxyStats': 'websocket',    // Periodic stats from ws-proxy message flow

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

  // StopTask Flow (5) - Detailed stopTask debugging
  'StopTask': 'stoptask',
  'StopTaskFlow': 'stoptask',
  'InFlightBid': 'stoptask',
  'LimiterQueue': 'stoptask',
  'BidAbort': 'stoptask',

  // Health (4)
  'Health': 'health',
  'HealthCheck': 'health',
  'ConnectivityMonitor': 'health',
  'EventLoop': 'health',  // Event loop lag monitoring - detects blocked event loop

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

  // TaskFlow - Consolidated task lifecycle logging (all go to taskflow-YYYY-MM-DD.log)
  // This category captures the ENTIRE task lifecycle for debugging stuck tasks
  // NOTE: JobProcessing, QueueEvent, TaskRecovery moved above to 'taskflow' category
  'TaskFlow': 'taskflow',           // General task flow events
  'StateTransition': 'taskflow',    // Status changes: STARTED → RUNNING → COMPLETED → SCHEDULED
  'ParentJob': 'taskflow',          // Parent job creation/tracking
  'LoopScheduler': 'taskflow',      // scheduleNextLoop() - timer creation
  'TaskCompletion': 'taskflow',     // checkTaskCompletionAndScheduleNext() - loop completion
  'TaskProgress': 'taskflow',       // updateTaskRunningPercentage() - progress calculation
  'RedisState': 'taskflow',         // persistTaskState() - Redis persistence
  'LockOp': 'taskflow',             // Lock acquisition/release for counters
  'TimerOp': 'taskflow',            // Timer creation/expiration/cleanup
  'StuckTaskCheck': 'taskflow',     // checkStuckTasks() - stuck task detection and recovery
  'TimerDiagnostic': 'taskflow',    // Periodic timer map diagnostics

  // WebSocket connection error logging (dedicated file for _doConnect() errors)
  'WsConnection': 'wsConnection',
};

// ============================================
// HELPER FUNCTIONS
// ============================================

export function getCategoryForPrefix(prefix: string): LogCategory {
  return PREFIX_TO_CATEGORY[prefix] || 'system';
}

export function getLogFilePath(prefix: string, date?: string): string {
  const logDate = date || new Date().toISOString().split('T')[0]; // YYYY-MM-DD
  const category = getCategoryForPrefix(prefix);
  return path.join(LOG_DIR, `${category}-${logDate}.log`);
}

export function getLogFilePathByCategory(category: LogCategory, date?: string): string {
  const logDate = date || new Date().toISOString().split('T')[0];
  return path.join(LOG_DIR, `${category}-${logDate}.log`);
}

function formatMetadata(metadata?: LogMetadata): string {
  if (!metadata || Object.keys(metadata).length === 0) return '';
  try {
    return ` | ${JSON.stringify(metadata)}`;
  } catch {
    return '';
  }
}

function writeToFile(logLine: string, prefix: string): void {
  // Skip file writes if we don't have write permissions
  if (!CAN_WRITE_LOGS) return;

  const filePath = getLogFilePath(prefix);

  // Add to buffer instead of synchronous write
  if (!logBuffer.has(filePath)) {
    logBuffer.set(filePath, []);
  }
  logBuffer.get(filePath)!.push(logLine);
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
// LOG LINE PARSER (for export functionality)
// ============================================

/**
 * Parse a single log line into structured format.
 * Supports both old format (no metadata) and new format (with JSON metadata).
 *
 * Old format: [2025-12-01T10:15:30.123Z] [ERROR][API] Message here
 * New format: [2025-12-01T10:15:30.123Z] [ERROR][API] Message here | {"taskId":"abc123"}
 */
export function parseLogLine(line: string, category?: LogCategory): ParsedLogLine | null {
  if (!line || line.trim() === '') return null;

  // Regex to parse log line: [timestamp] [LEVEL][PREFIX] message | {metadata}
  // Handles both DEBUG format [PREFIX] and other formats [LEVEL][PREFIX]
  const regex = /^\[(\d{4}-\d{2}-\d{2}T[\d:.]+Z)\]\s+(?:\[(ERROR|WARN|INFO)\])?\[([^\]]+)\]\s+(.+)$/;
  const match = line.match(regex);

  if (!match) {
    // Fallback: try simpler pattern for debug logs without level
    const simpleRegex = /^\[(\d{4}-\d{2}-\d{2}T[\d:.]+Z)\]\s+\[([^\]]+)\]\s+(.+)$/;
    const simpleMatch = line.match(simpleRegex);

    if (!simpleMatch) return null;

    const [, timestamp, prefix, messageWithMeta] = simpleMatch;
    const { message, metadata } = extractMetadataFromMessage(messageWithMeta);

    return {
      timestamp,
      level: 'DEBUG',
      prefix,
      category: category || getCategoryForPrefix(prefix),
      message,
      metadata,
      raw: line
    };
  }

  const [, timestamp, level, prefix, messageWithMeta] = match;
  const { message, metadata } = extractMetadataFromMessage(messageWithMeta);

  return {
    timestamp,
    level: (level as 'ERROR' | 'WARN' | 'INFO') || 'DEBUG',
    prefix,
    category: category || getCategoryForPrefix(prefix),
    message,
    metadata,
    raw: line
  };
}

/**
 * Extract JSON metadata from message if present.
 * Message format: "Actual message | {json metadata}"
 */
function extractMetadataFromMessage(messageWithMeta: string): { message: string; metadata?: LogMetadata } {
  // Look for JSON metadata at the end after " | "
  const metadataMatch = messageWithMeta.match(/^(.+?)\s+\|\s+(\{.+\})$/);

  if (metadataMatch) {
    const [, message, jsonStr] = metadataMatch;
    try {
      const metadata = JSON.parse(jsonStr) as LogMetadata;
      return { message: message.trim(), metadata };
    } catch {
      // JSON parse failed, return original message
      return { message: messageWithMeta };
    }
  }

  // No JSON metadata, try to extract key=value pairs from message
  const metadata = extractKeyValuePairs(messageWithMeta);
  return { message: messageWithMeta, metadata: Object.keys(metadata).length > 0 ? metadata : undefined };
}

/**
 * Extract key=value pairs from log message for backward compatibility.
 * Looks for patterns like: taskId=abc123 | marketplace=opensea | slug=my-collection
 */
function extractKeyValuePairs(message: string): LogMetadata {
  const metadata: LogMetadata = {};

  // Match patterns like: taskId=abc123, marketplace=opensea, slug=my-collection
  const patterns = [
    { key: 'taskId', regex: /taskId[=:][\s]*([a-f0-9]{24}|[a-zA-Z0-9_-]+)/i },
    { key: 'marketplace', regex: /marketplace[=:][\s]*(blur|opensea|magiceden)/i },
    { key: 'slug', regex: /slug[=:][\s]*([a-zA-Z0-9_-]+)/i },
    { key: 'tokenId', regex: /tokenId[=:][\s]*(\d+)/i },
    { key: 'traitKey', regex: /traitKey[=:][\s]*([^\s|]+)/i },
    { key: 'jobId', regex: /jobId[=:][\s]*([^\s|]+)/i },
    { key: 'identifier', regex: /identifier[=:][\s]*([^\s|]+)/i },
  ];

  for (const { key, regex } of patterns) {
    const match = message.match(regex);
    if (match) {
      metadata[key] = match[1].toLowerCase() === 'blur' || match[1].toLowerCase() === 'opensea' || match[1].toLowerCase() === 'magiceden'
        ? match[1].toLowerCase() as 'blur' | 'opensea' | 'magiceden'
        : match[1];
    }
  }

  return metadata;
}

/**
 * Get all log categories.
 */
export function getAllCategories(): LogCategory[] {
  return [
    'startup', 'task', 'scheduler', 'bidflow', 'counterbid', 'queue', 'worker',
    'database', 'redis', 'lock', 'blur', 'opensea', 'magiceden', 'api', 'websocket',
    'listing', 'collection', 'balance', 'cancellation', 'health', 'metadata', 'stoptask',
    'taskflow', 'system', 'wsConnection'
  ];
}

/**
 * Map marketplace filter to relevant log categories.
 * When filtering by marketplace, we search multiple related categories.
 */
export function getMarketplaceCategories(marketplace: string): LogCategory[] {
  const categoryMap: Record<string, LogCategory[]> = {
    'opensea': ['opensea', 'bidflow', 'counterbid', 'queue', 'api', 'cancellation', 'stoptask'],
    'blur': ['blur', 'bidflow', 'counterbid', 'queue', 'api', 'cancellation', 'stoptask'],
    'magiceden': ['magiceden', 'bidflow', 'counterbid', 'queue', 'api', 'cancellation', 'stoptask'],
    'system': ['system', 'startup', 'database', 'redis', 'lock', 'task', 'scheduler', 'stoptask'],
    'health': ['health', 'worker', 'scheduler', 'queue'],
    'stoptask': ['stoptask', 'cancellation', 'queue', 'task'],
    'all': getAllCategories(),
  };
  return categoryMap[marketplace.toLowerCase()] || getAllCategories();
}

// ============================================
// PUBLIC API (with optional metadata support)
// ============================================

/**
 * Log a debug message to both console and file.
 * Only logs when DEBUG=true.
 * EXCEPTION: 'taskflow' category always logs (critical for debugging stuck tasks).
 * Routes to category-specific log file based on prefix.
 *
 * @param prefix - Log prefix category (e.g., 'TaskScheduler', 'BidFlow', 'STARTUP')
 * @param message - Log message content
 * @param metadata - Optional structured metadata for filtering (taskId, marketplace, slug, etc.)
 */
export function debugLog(prefix: string, message: string, metadata?: LogMetadata): void {
  // Always log taskflow category (critical for debugging stuck tasks)
  const category = getCategoryForPrefix(prefix);
  const isTaskflow = category === 'taskflow';

  // Skip non-taskflow logs if DEBUG is off
  if (!isTaskflow && !DEBUG) return;

  const timestamp = new Date().toISOString();
  const metaStr = formatMetadata(metadata);
  const logLine = `[${timestamp}] [${prefix}] ${message}${metaStr}`;

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
 * @param metadata - Optional structured metadata for filtering (taskId, marketplace, slug, etc.)
 */
export function errorLog(prefix: string, message: string, error?: unknown, metadata?: LogMetadata): void {
  const timestamp = new Date().toISOString();
  const errorDetails = error ? ` | ${formatError(error)}` : '';
  const metaStr = formatMetadata(metadata);
  const logLine = `[${timestamp}] [ERROR][${prefix}] ${message}${errorDetails}${metaStr}`;

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
 * @param metadata - Optional structured metadata for filtering (taskId, marketplace, slug, etc.)
 */
export function warnLog(prefix: string, message: string, metadata?: LogMetadata): void {
  const timestamp = new Date().toISOString();
  const metaStr = formatMetadata(metadata);
  const logLine = `[${timestamp}] [WARN][${prefix}] ${message}${metaStr}`;

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
 * @param metadata - Optional structured metadata for filtering (taskId, marketplace, slug, etc.)
 */
export function infoLog(prefix: string, message: string, metadata?: LogMetadata): void {
  const timestamp = new Date().toISOString();
  const metaStr = formatMetadata(metadata);
  const logLine = `[${timestamp}] [INFO][${prefix}] ${message}${metaStr}`;

  // Console output
  console.log(logLine);

  // File output (append to category-specific file)
  writeToFile(logLine, prefix);
}
