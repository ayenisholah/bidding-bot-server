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

  log(entry: Omit<MarketLogEntry, 'timestamp' | 'marketplace'>) {
    const row: MarketLogEntry = {
      ...entry,
      marketplace: this.marketplace,
      timestamp: new Date().toISOString(),
    };

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

    if (filter.timeRange && !startTime) {
      const map: Record<string, number> = { '5m': 5, '10m': 10 };
      if (filter.timeRange in map) startTime = new Date(now.getTime() - map[filter.timeRange!] * 60 * 1000);
      else if (filter.timeRange === '1h') startTime = new Date(now.getTime() - 60 * 60 * 1000);
      else if (filter.timeRange === '24h') startTime = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    }

    const files = fs.readdirSync(this.logsDir)
      .filter(f => f.startsWith(`${this.marketplace}-`) && f.endsWith('.log'))
      .sort();

    for (const file of files) {
      const contents = fs.readFileSync(path.join(this.logsDir, file), 'utf-8');
      for (const line of contents.split('\n')) {
        if (!line.trim()) continue;
        try {
          const entry = JSON.parse(line) as MarketLogEntry;
          const t = new Date(entry.timestamp);
          if (startTime && t < startTime) continue;
          if (endTime && t > endTime) continue;
          if (filter.taskId && entry.taskId !== filter.taskId) continue;
          if (filter.level && filter.level !== 'all' && entry.level !== filter.level) continue;
          if (filter.function && entry.function !== filter.function) continue;
          logs.push(entry);
        } catch (err) {
          console.error(`[${this.marketplace.toUpperCase()} Logger] parse error`, err);
        }
      }
    }

    return logs.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
  }

  async exportLogs(options: MarketLogFilter & { format: 'json' | 'csv' }): Promise<Buffer> {
    const logs = await this.getLogs(options);
    if (options.format === 'json') return Buffer.from(JSON.stringify(logs, null, 2));

    const headers = ['timestamp', 'marketplace', 'taskId', 'function', 'stage', 'level', 'message', 'metadata'];
    const lines = [headers.join(',')];
    for (const l of logs) {
      const row = [
        l.timestamp,
        l.marketplace,
        l.taskId,
        l.function,
        l.stage,
        l.level,
        `"${String(l.message).replace(/"/g, '""')}"`,
        `"${JSON.stringify(l.metadata).replace(/"/g, '""')}"`,
      ];
      lines.push(row.join(','));
    }
    return Buffer.from(lines.join('\n'));
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
