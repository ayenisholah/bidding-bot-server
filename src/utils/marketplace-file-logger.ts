import * as fs from 'fs';
import * as path from 'path';
import { MarketLogEntry, MarketLogFilter, MarketLogLevel, MarketLogStage, Marketplace } from '../types/marketplace-logs.types';

class MarketplaceFileLogger {
  private readonly logsDir: string;
  private currentLogFile: string;
  private writeStream: fs.WriteStream | null = null;
  private rotationTimer: NodeJS.Timeout | null = null;
  private readonly retentionDays = 1; // 1-day automatic cleanup

  constructor(private readonly marketplace: Marketplace) {
    this.logsDir = path.join(process.cwd(), 'logs', marketplace);
    this.currentLogFile = this.getLogFileName();
    this.initialize();
  }

  private initialize() {
    if (!fs.existsSync(this.logsDir)) {
      fs.mkdirSync(this.logsDir, { recursive: true });
    }
    this.createWriteStream();
    this.setupRotation();
    this.cleanupOldLogs();
  }

  private getLogFileName(): string {
    const dateStr = new Date().toISOString().split('T')[0];
    return path.join(this.logsDir, `${this.marketplace}-${dateStr}.log`);
  }

  private createWriteStream() {
    if (this.writeStream) this.writeStream.end();
    const file = this.getLogFileName();
    this.writeStream = fs.createWriteStream(file, { flags: 'a' });
    this.currentLogFile = file;
  }

  private setupRotation() {
    this.rotationTimer = setInterval(() => {
      const next = this.getLogFileName();
      if (next !== this.currentLogFile) {
        this.createWriteStream();
        this.cleanupOldLogs();
      }
    }, 60 * 60 * 1000); // hourly check
  }

  private cleanupOldLogs() {
    try {
      const files = fs.readdirSync(this.logsDir);
      const now = Date.now();
      const maxAge = this.retentionDays * 24 * 60 * 60 * 1000;
      for (const file of files) {
        if (file.startsWith(`${this.marketplace}-`) && file.endsWith('.log')) {
          const p = path.join(this.logsDir, file);
          const age = now - fs.statSync(p).mtime.getTime();
          if (age > maxAge) fs.unlinkSync(p);
        }
      }
    } catch (err) {
      // Non-fatal
      console.error(`[${this.marketplace.toUpperCase()} Logger] cleanup error`, err);
    }
  }

  private shouldSuppressEntry(entry: MarketLogEntry): boolean {
    // Drop repetitive task-state noise entirely to keep logs actionable
    return (
      entry.level === 'warn' &&
      typeof entry.message === 'string' &&
      entry.message.toLowerCase().startsWith('task not running')
    );
  }

  log(entry: Omit<MarketLogEntry, 'timestamp' | 'marketplace'>) {
    const row: MarketLogEntry = {
      ...entry,
      marketplace: this.marketplace,
      timestamp: new Date().toISOString(),
    };

    if (this.shouldSuppressEntry(row)) return;

    if (this.writeStream) {
      this.writeStream.write(JSON.stringify(row) + '\n');
    }

    if (process.env.NODE_ENV !== 'production') {
      const prefix = `[${row.level.toUpperCase()}][${row.marketplace}][${row.function}][${row.stage}]`;
      const msg = `${prefix} ${row.message}`;
      switch (row.level) {
        case 'error':
          console.error(msg, row.metadata);
          break;
        case 'warn':
          console.warn(msg, row.metadata);
          break;
        case 'debug':
          if (process.env.DEBUG === 'true') console.debug(msg, row.metadata);
          break;
        default:
          console.log(msg);
      }
    }
  }

  async getLogs(filter: MarketLogFilter = {}): Promise<MarketLogEntry[]> {
    const logs: MarketLogEntry[] = [];
    const now = new Date();
    let { startTime, endTime } = filter;
    endTime = endTime || now;

    // Validate and set time range with bounds checking (max 30 days)
    if (filter.timeRange && !startTime) {
      const map: Record<string, number> = { '5m': 5, '10m': 10 };
      if (filter.timeRange in map) startTime = new Date(now.getTime() - map[filter.timeRange!] * 60 * 1000);
      else if (filter.timeRange === '1h') startTime = new Date(now.getTime() - 60 * 60 * 1000);
      else if (filter.timeRange === '24h') startTime = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    }

    // Enforce maximum time range of 1 day
    const maxTimeRange = 1 * 24 * 60 * 60 * 1000; // 1 day in milliseconds
    if (startTime && endTime) {
      const timeRange = endTime.getTime() - startTime.getTime();
      if (timeRange > maxTimeRange) {
        startTime = new Date(endTime.getTime() - maxTimeRange);
      }
    }

    try {
      // Check if logs directory exists
      if (!fs.existsSync(this.logsDir)) {
        console.warn(`[${this.marketplace.toUpperCase()} Logger] Logs directory not found: ${this.logsDir}`);
        return [];
      }

      const files = fs.readdirSync(this.logsDir)
        .filter(f => f.startsWith(`${this.marketplace}-`) && f.endsWith('.log'))
        .sort();

      // Process files that might contain logs within the time range
      for (const file of files) {
        // Skip files that are definitely outside our time range based on filename
        const dateMatch = file.match(/(\d{4}-\d{2}-\d{2})\.log$/);
        if (dateMatch && startTime) {
          const fileDate = new Date(dateMatch[1] + 'T00:00:00Z');
          if (fileDate > new Date(endTime.getTime() + 24 * 60 * 60 * 1000)) continue;
          if (fileDate < new Date(startTime.getTime() - 24 * 60 * 60 * 1000)) continue;
        }

        try {
          const filePath = path.join(this.logsDir, file);
          const stats = fs.statSync(filePath);

          // Skip very large files (> 100MB) with a warning
          if (stats.size > 100 * 1024 * 1024) {
            console.warn(`[${this.marketplace.toUpperCase()} Logger] Skipping large log file: ${file} (${(stats.size / 1024 / 1024).toFixed(2)}MB)`);
            continue;
          }

          const contents = fs.readFileSync(filePath, 'utf-8');
          const lines = contents.split('\n');

          for (const line of lines) {
            if (!line.trim()) continue;
            try {
              const entry = JSON.parse(line) as MarketLogEntry;
              const t = new Date(entry.timestamp);

              // Validate timestamp
              if (isNaN(t.getTime())) continue;

              if (startTime && t < startTime) continue;
              if (endTime && t > endTime) continue;
              if (filter.taskId && entry.taskId !== filter.taskId) continue;
              if (filter.level && filter.level !== 'all' && entry.level !== filter.level) continue;
              if (filter.function && entry.function !== filter.function) continue;
              logs.push(entry);

              // Keep only the latest 50,000 logs using sliding window
              if (logs.length > 50000) {
                logs.shift(); // Remove oldest entry to maintain the limit
              }
            } catch (err) {
              // Silently skip malformed log entries
              if (process.env.DEBUG === 'true') {
                console.error(`[${this.marketplace.toUpperCase()} Logger] Parse error in ${file}:`, err);
              }
            }
          }
        } catch (fileErr) {
          console.error(`[${this.marketplace.toUpperCase()} Logger] Error reading file ${file}:`, fileErr);
        }
      }

      // Log if we hit the limit
      if (logs.length === 50000) {
        console.warn(`[${this.marketplace.toUpperCase()} Logger] Returning latest 50,000 logs (sliding window applied)`);
      }
    } catch (err) {
      console.error(`[${this.marketplace.toUpperCase()} Logger] Error in getLogs:`, err);
      throw new Error(`Failed to retrieve logs: ${err instanceof Error ? err.message : 'Unknown error'}`);
    }

    return logs.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
  }

  async exportLogs(options: MarketLogFilter & { format: 'json' | 'csv' }): Promise<Buffer> {
    try {
      const logs = await this.getLogs(options);

      if (options.format === 'json') {
        // Sanitize logs to prevent XSS in JSON exports
        const sanitizedLogs = logs.map(log => ({
          ...log,
          message: this.sanitizeForExport(String(log.message)),
          metadata: this.sanitizeMetadata(log.metadata)
        }));
        return Buffer.from(JSON.stringify(sanitizedLogs, null, 2));
      }

      // CSV export with proper escaping
      const headers = ['timestamp', 'marketplace', 'taskId', 'function', 'stage', 'level', 'message', 'metadata'];
      const lines: string[] = [headers.map(h => this.escapeCsvField(h)).join(',')];

      for (const l of logs) {
        const row = [
          this.escapeCsvField(l.timestamp),
          this.escapeCsvField(l.marketplace),
          this.escapeCsvField(l.taskId),
          this.escapeCsvField(l.function),
          this.escapeCsvField(l.stage),
          this.escapeCsvField(l.level),
          this.escapeCsvField(String(l.message)),
          this.escapeCsvField(JSON.stringify(l.metadata))
        ];
        lines.push(row.join(','));
      }

      // Add BOM for Excel compatibility
      const bom = '\uFEFF';
      return Buffer.from(bom + lines.join('\n'), 'utf8');
    } catch (error) {
      console.error(`[${this.marketplace.toUpperCase()} Logger] Export error:`, error);
      throw new Error(`Failed to export logs: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  private escapeCsvField(field: string): string {
    // Convert to string and handle null/undefined
    const str = field != null ? String(field) : '';

    // Check if field needs escaping
    if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
      // Escape double quotes by doubling them
      const escaped = str.replace(/"/g, '""');
      // Wrap in quotes
      return `"${escaped}"`;
    }

    return str;
  }

  private sanitizeForExport(str: string): string {
    // Remove potential XSS vectors while preserving readability
    return str
      .replace(/[<>]/g, '') // Remove angle brackets
      .replace(/javascript:/gi, '') // Remove javascript protocol
      .replace(/on\w+\s*=/gi, ''); // Remove event handlers
  }

  private sanitizeMetadata(metadata: any): any {
    if (typeof metadata === 'string') {
      return this.sanitizeForExport(metadata);
    }
    if (Array.isArray(metadata)) {
      return metadata.map(item => this.sanitizeMetadata(item));
    }
    if (metadata && typeof metadata === 'object') {
      const sanitized: any = {};
      for (const [key, value] of Object.entries(metadata)) {
        sanitized[this.sanitizeForExport(key)] = this.sanitizeMetadata(value);
      }
      return sanitized;
    }
    return metadata;
  }

  destroy() {
    if (this.writeStream) this.writeStream.end();
    if (this.rotationTimer) clearInterval(this.rotationTimer);
  }
}

const instances: Partial<Record<Marketplace, MarketplaceFileLogger>> = {};

export function getMarketplaceLogger(marketplace: Marketplace): MarketplaceFileLogger {
  if (!instances[marketplace]) instances[marketplace] = new MarketplaceFileLogger(marketplace);
  return instances[marketplace]!;
}

// Raw logger passthrough to centralize remaining direct calls
export function logRaw(
  marketplace: Marketplace,
  entry: Omit<MarketLogEntry, 'timestamp' | 'marketplace'>
) {
  try {
    getMarketplaceLogger(marketplace).log(entry);
  } catch (e) {
    if (process.env.DEBUG === 'true') {
      console.error(`[${marketplace.toUpperCase()} Logger] logRaw failed`, e);
    }
  }
}

// Convenience helpers for consistent structured logging
export function logEarlyReturn(
  marketplace: Marketplace,
  taskId: string,
  fn: string,
  message: string,
  metadata: Record<string, any> = {}
) {
  try {
    getMarketplaceLogger(marketplace).log({
      taskId,
      function: fn,
      stage: 'early_return',
      level: 'warn',
      message,
      metadata,
    });
  } catch (e) {
    if (process.env.DEBUG === 'true') {
      console.error(`[${marketplace.toUpperCase()} Logger] logEarlyReturn failed`, e);
    }
  }
}

export function logDecision(
  marketplace: Marketplace,
  taskId: string,
  fn: string,
  stage: MarketLogStage,
  message: string,
  metadata: Record<string, any> = {},
  level: MarketLogLevel = 'info'
) {
  try {
    getMarketplaceLogger(marketplace).log({
      taskId,
      function: fn,
      stage,
      level,
      message,
      metadata,
    });
  } catch (e) {
    if (process.env.DEBUG === 'true') {
      console.error(`[${marketplace.toUpperCase()} Logger] logDecision failed`, e);
    }
  }
}

export function logError(
  marketplace: Marketplace,
  taskId: string,
  fn: string,
  message: string,
  error?: unknown,
  metadata: Record<string, any> = {}
) {
  try {
    const err = error as any;
    const meta = {
      ...metadata,
      errorMessage: err?.message || String(error ?? ''),
      errorStack: err?.stack,
    };
    getMarketplaceLogger(marketplace).log({
      taskId,
      function: fn,
      stage: 'error',
      level: 'error',
      message,
      metadata: meta,
    });
  } catch (e) {
    if (process.env.DEBUG === 'true') {
      console.error(`[${marketplace.toUpperCase()} Logger] logError failed`, e);
    }
  }
}

// System-level logging functions
export function logSystemError(
  fn: string,
  message: string,
  error?: unknown,
  metadata: Record<string, any> = {}
) {
  try {
    const err = error as any;
    const meta = {
      ...metadata,
      errorMessage: err?.message || String(error ?? ''),
      errorStack: err?.stack,
      errorCode: err?.code,
    };
    getMarketplaceLogger('system').log({
      taskId: 'N/A',
      function: fn,
      stage: 'error',
      level: 'error',
      message,
      metadata: meta,
    });
  } catch (err) {
    // Fallback to console if logging fails
    console.error(`[SYSTEM] ${fn}: ${message}`, error ?? err);
  }
}

export function logSystemInfo(
  fn: string,
  message: string,
  metadata: Record<string, any> = {}
) {
  try {
    getMarketplaceLogger('system').log({
      taskId: 'N/A',
      function: fn,
      stage: 'processing',
      level: 'info',
      message,
      metadata,
    });
  } catch (err) {
    if (process.env.DEBUG === 'true') {
      console.log(`[SYSTEM] ${fn}: ${message}`, err);
    }
  }
}

export function logSystemWarn(
  fn: string,
  message: string,
  metadata: Record<string, any> = {}
) {
  try {
    getMarketplaceLogger('system').log({
      taskId: 'N/A',
      function: fn,
      stage: 'processing',
      level: 'warn',
      message,
      metadata,
    });
  } catch (err) {
    if (process.env.DEBUG === 'true') {
      console.warn(`[SYSTEM] ${fn}: ${message}`, err);
    }
  }
}
