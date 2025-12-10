import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import {
  LogCategory,
  ParsedLogLine,
  parseLogLine,
  getAllCategories,
  getMarketplaceCategories,
  getLogFilePathByCategory,
} from './debug-file-logger';

// ============================================
// FILTER INTERFACES
// ============================================

export interface LogFilters {
  taskId?: string;
  marketplace?: 'blur' | 'opensea' | 'magiceden' | 'system' | 'health' | 'all';
  category?: LogCategory;
  level?: 'all' | 'info' | 'warn' | 'error' | 'debug';
  timeRange?: '5m' | '10m' | '1h' | '24h' | 'custom';
  startTime?: string; // ISO string
  endTime?: string; // ISO string
  limit?: number;
  offset?: number;
}

export interface LogExportResult {
  logs: ParsedLogLine[];
  metadata: {
    total: number;
    limit: number;
    offset: number;
    truncated: boolean;
    filters: LogFilters;
    filesSearched: string[];
    exportedAt: string;
  };
}

// ============================================
// TIME RANGE HELPERS
// ============================================

/**
 * Calculate start and end times based on time range filter.
 */
export function getTimeRange(filters: LogFilters): { startTime: Date; endTime: Date } {
  const now = new Date();
  let startTime: Date;
  let endTime: Date = now;

  if (filters.timeRange === 'custom' && filters.startTime) {
    startTime = new Date(filters.startTime);
    endTime = filters.endTime ? new Date(filters.endTime) : now;
  } else {
    switch (filters.timeRange) {
      case '5m':
        startTime = new Date(now.getTime() - 5 * 60 * 1000);
        break;
      case '10m':
        startTime = new Date(now.getTime() - 10 * 60 * 1000);
        break;
      case '1h':
        startTime = new Date(now.getTime() - 60 * 60 * 1000);
        break;
      case '24h':
        startTime = new Date(now.getTime() - 24 * 60 * 60 * 1000);
        break;
      default:
        // Default to 1 hour
        startTime = new Date(now.getTime() - 60 * 60 * 1000);
    }
  }

  return { startTime, endTime };
}

/**
 * Get all dates between start and end time as YYYY-MM-DD strings.
 */
export function getDateRange(startTime: Date, endTime: Date): string[] {
  const dates: string[] = [];
  const current = new Date(startTime);
  current.setUTCHours(0, 0, 0, 0);

  while (current <= endTime) {
    dates.push(current.toISOString().split('T')[0]);
    current.setDate(current.getDate() + 1);
  }

  return dates;
}

// ============================================
// FILE DISCOVERY
// ============================================

/**
 * Determine which log files to search based on filters.
 */
export function getLogFilesToSearch(filters: LogFilters): string[] {
  const { startTime, endTime } = getTimeRange(filters);
  const dates = getDateRange(startTime, endTime);
  const files: string[] = [];

  // Determine categories to search
  let categories: LogCategory[];
  if (filters.category) {
    categories = [filters.category];
  } else if (filters.marketplace && filters.marketplace !== 'all') {
    categories = getMarketplaceCategories(filters.marketplace);
  } else {
    categories = getAllCategories();
  }

  // Build file paths
  for (const date of dates) {
    for (const category of categories) {
      const filePath = getLogFilePathByCategory(category, date);
      if (fs.existsSync(filePath)) {
        files.push(filePath);
      }
    }
  }

  return files;
}

// ============================================
// FILTER MATCHING
// ============================================

/**
 * Check if a parsed log line matches the filters.
 */
export function matchesFilters(log: ParsedLogLine, filters: LogFilters, startTime: Date, endTime: Date): boolean {
  // Time filter
  const logTime = new Date(log.timestamp);
  if (logTime < startTime || logTime > endTime) {
    return false;
  }

  // Level filter
  if (filters.level && filters.level !== 'all') {
    const levelMatch = filters.level.toUpperCase() === log.level;
    if (!levelMatch) return false;
  }

  // TaskId filter (check both metadata and message)
  if (filters.taskId) {
    const hasTaskIdInMeta = log.metadata?.taskId === filters.taskId;
    const hasTaskIdInMessage = log.message.includes(filters.taskId) || log.raw.includes(filters.taskId);
    if (!hasTaskIdInMeta && !hasTaskIdInMessage) {
      return false;
    }
  }

  // Marketplace filter (check metadata)
  if (filters.marketplace && filters.marketplace !== 'all' && filters.marketplace !== 'system' && filters.marketplace !== 'health') {
    const hasMarketplaceInMeta = log.metadata?.marketplace === filters.marketplace;
    const hasMarketplaceInMessage = log.message.toLowerCase().includes(filters.marketplace) || log.raw.toLowerCase().includes(filters.marketplace);
    const categoryMatches = getMarketplaceCategories(filters.marketplace).includes(log.category);
    if (!hasMarketplaceInMeta && !hasMarketplaceInMessage && !categoryMatches) {
      return false;
    }
  }

  return true;
}

// ============================================
// STREAMING FILE PARSER
// ============================================

/**
 * Parse a log file line-by-line using streaming.
 * Yields parsed log lines that match filters.
 */
export async function* parseLogFileStreaming(
  filePath: string,
  filters: LogFilters,
  startTime: Date,
  endTime: Date
): AsyncGenerator<ParsedLogLine> {
  // Extract category from filename (e.g., "bidflow-2025-12-01.log" -> "bidflow")
  const filename = path.basename(filePath);
  const categoryMatch = filename.match(/^([a-z]+)-\d{4}-\d{2}-\d{2}\.log$/);
  const category = categoryMatch ? (categoryMatch[1] as LogCategory) : undefined;

  const fileStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const parsed = parseLogLine(line, category);
    if (parsed && matchesFilters(parsed, filters, startTime, endTime)) {
      yield parsed;
    }
  }
}

// ============================================
// MAIN EXPORT FUNCTION
// ============================================

const DEFAULT_LIMIT = 10000;
const MAX_LIMIT = 50000;

/**
 * Export logs with filters.
 * Uses streaming to handle large files efficiently.
 */
export async function exportLogs(filters: LogFilters): Promise<LogExportResult> {
  const limit = Math.min(filters.limit || DEFAULT_LIMIT, MAX_LIMIT);
  const offset = filters.offset || 0;
  const { startTime, endTime } = getTimeRange(filters);
  const files = getLogFilesToSearch(filters);

  const logs: ParsedLogLine[] = [];
  let totalScanned = 0;
  let skipped = 0;

  // Process files and collect logs
  for (const file of files) {
    if (logs.length >= limit) break;

    for await (const log of parseLogFileStreaming(file, filters, startTime, endTime)) {
      totalScanned++;

      // Handle offset (skip first N matches)
      if (skipped < offset) {
        skipped++;
        continue;
      }

      logs.push(log);

      if (logs.length >= limit) break;
    }
  }

  // Sort by timestamp (most recent first)
  logs.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());

  return {
    logs,
    metadata: {
      total: totalScanned,
      limit,
      offset,
      truncated: totalScanned > limit + offset,
      filters,
      filesSearched: files,
      exportedAt: new Date().toISOString(),
    },
  };
}

// ============================================
// CSV EXPORT
// ============================================

/**
 * Convert parsed logs to CSV format.
 */
export function logsToCSV(logs: ParsedLogLine[]): string {
  const headers = ['timestamp', 'level', 'prefix', 'category', 'marketplace', 'taskId', 'slug', 'tokenId', 'message'];
  const rows = [headers.join(',')];

  for (const log of logs) {
    const row = [
      log.timestamp,
      log.level,
      log.prefix,
      log.category,
      log.metadata?.marketplace || '',
      log.metadata?.taskId || '',
      log.metadata?.slug || '',
      log.metadata?.tokenId?.toString() || '',
      // Escape message for CSV (wrap in quotes, escape internal quotes)
      `"${log.message.replace(/"/g, '""')}"`,
    ];
    rows.push(row.join(','));
  }

  return rows.join('\n');
}

/**
 * Convert parsed logs to JSON format.
 */
export function logsToJSON(result: LogExportResult): string {
  return JSON.stringify(result, null, 2);
}
