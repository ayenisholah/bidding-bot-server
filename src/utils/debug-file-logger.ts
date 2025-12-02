import * as fs from 'fs';
import * as path from 'path';

const DEBUG = process.env.DEBUG === 'true';
export const LOG_DIR = path.resolve(process.cwd(), 'debug-logs');

// Ensure directory exists on module load
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

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
// LOG CATEGORY DEFINITIONS (23 categories)
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
  | 'system';

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

  // StopTask Flow (5) - Detailed stopTask debugging
  'StopTask': 'stoptask',
  'StopTaskFlow': 'stoptask',
  'InFlightBid': 'stoptask',
  'LimiterQueue': 'stoptask',
  'BidAbort': 'stoptask',

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
    'listing', 'collection', 'balance', 'cancellation', 'health', 'metadata', 'stoptask', 'system'
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
 * Only logs when DEBUG=true environment variable is set.
 * Routes to category-specific log file based on prefix.
 *
 * @param prefix - Log prefix category (e.g., 'TaskScheduler', 'BidFlow', 'STARTUP')
 * @param message - Log message content
 * @param metadata - Optional structured metadata for filtering (taskId, marketplace, slug, etc.)
 */
export function debugLog(prefix: string, message: string, metadata?: LogMetadata): void {
  if (!DEBUG) return;

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
