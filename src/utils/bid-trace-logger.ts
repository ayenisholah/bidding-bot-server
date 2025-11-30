import * as fs from 'fs';
import * as path from 'path';
import { warnLog } from './debug-file-logger';
import {
  BidTraceEntry,
  BidTraceFilter,
  BidTraceLevel,
  BidTraceStage,
  BidTraceMarketplace,
  BidTraceBidType,
  BidTraceQueryResult,
  BidTraceDuplicateGroup,
  BidTraceSummary,
  BidTraceContext,
} from '../types/bid-trace.types';

/**
 * Bid Trace Logger
 *
 * Comprehensive logging system for tracing bid attempts through their complete lifecycle.
 * Enables diagnosis of duplicate bid issues by correlating events from WebSocket receipt
 * through job processing to API completion.
 */
class BidTraceLogger {
  private readonly logsDir: string;
  private currentLogFile: string;
  private writeStream: fs.WriteStream | null = null;
  private rotationTimer: NodeJS.Timeout | null = null;
  private readonly retentionDays = 1;

  // In-memory trace contexts for elapsed time calculation (5-minute TTL)
  private traceContexts: Map<string, { startTime: number; correlationId: string; taskId: string }> =
    new Map();
  private contextCleanupTimer: NodeJS.Timeout | null = null;

  // Environment config
  private readonly enabled: boolean;
  private readonly minLevel: BidTraceLevel;

  private readonly levelPriority: Record<BidTraceLevel, number> = {
    trace: 0,
    debug: 1,
    info: 2,
    warn: 3,
    error: 4,
  };

  constructor() {
    this.logsDir = path.join(process.cwd(), 'logs', 'bid-traces');
    this.currentLogFile = this.getLogFileName();
    this.enabled = process.env.BID_TRACE_ENABLED !== 'false';
    this.minLevel = (process.env.BID_TRACE_LEVEL as BidTraceLevel) || 'info';
    this.initialize();
  }

  private initialize() {
    if (!this.enabled) return;

    if (!fs.existsSync(this.logsDir)) {
      fs.mkdirSync(this.logsDir, { recursive: true });
    }
    this.createWriteStream();
    this.setupRotation();
    this.cleanupOldLogs();
    this.setupContextCleanup();
  }

  private getLogFileName(): string {
    const dateStr = new Date().toISOString().split('T')[0];
    return path.join(this.logsDir, `bid-traces-${dateStr}.log`);
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
      if (!fs.existsSync(this.logsDir)) return;
      const files = fs.readdirSync(this.logsDir);
      const now = Date.now();
      const maxAge = this.retentionDays * 24 * 60 * 60 * 1000;
      for (const file of files) {
        if (file.startsWith('bid-traces-') && file.endsWith('.log')) {
          const p = path.join(this.logsDir, file);
          const age = now - fs.statSync(p).mtime.getTime();
          if (age > maxAge) fs.unlinkSync(p);
        }
      }
    } catch {
      // Non-fatal
    }
  }

  private setupContextCleanup() {
    // Clean up old trace contexts every minute
    this.contextCleanupTimer = setInterval(() => {
      const now = Date.now();
      const maxAge = 5 * 60 * 1000; // 5 minutes
      for (const [traceId, context] of this.traceContexts.entries()) {
        if (now - context.startTime > maxAge) {
          this.traceContexts.delete(traceId);
        }
      }
    }, 60 * 1000);
  }

  private shouldLog(level: BidTraceLevel): boolean {
    if (!this.enabled) return false;
    return this.levelPriority[level] >= this.levelPriority[this.minLevel];
  }

  /**
   * Generate a unique trace ID
   */
  generateTraceId(): string {
    return `bt-${Date.now()}-${Math.random().toString(36).substring(2, 8)}`;
  }

  /**
   * Create a new trace when a bid attempt begins
   */
  createTrace(params: {
    correlationId?: string;
    marketplace: BidTraceMarketplace;
    taskId: string;
    collectionSlug: string;
    contractAddress?: string;
    bidType: BidTraceBidType;
    tokenId?: string;
    traitKey?: string;
    orderHash?: string;
  }): string {
    const traceId = this.generateTraceId();
    const correlationId = params.correlationId || params.orderHash || traceId;
    const startTime = Date.now();

    this.traceContexts.set(traceId, {
      startTime,
      correlationId,
      taskId: params.taskId,
    });

    return traceId;
  }

  /**
   * Get trace context for elapsed time calculation
   */
  getTraceContext(traceId: string): BidTraceContext | null {
    const ctx = this.traceContexts.get(traceId);
    if (!ctx) return null;
    return {
      traceId,
      correlationId: ctx.correlationId,
      startTime: ctx.startTime,
    };
  }

  /**
   * Core logging method
   */
  log(
    traceId: string,
    stage: BidTraceStage,
    level: BidTraceLevel,
    data: Partial<BidTraceEntry>
  ): void {
    if (!this.shouldLog(level)) return;

    const ctx = this.traceContexts.get(traceId);
    const now = Date.now();

    const entry: BidTraceEntry = {
      traceId,
      correlationId: ctx?.correlationId || data.correlationId || traceId,
      timestamp: new Date().toISOString(),
      elapsedMs: ctx ? now - ctx.startTime : undefined,
      stage,
      level,
      marketplace: data.marketplace || ('opensea' as BidTraceMarketplace),
      taskId: ctx?.taskId || data.taskId || 'unknown',
      collectionSlug: data.collectionSlug || 'unknown',
      bidType: data.bidType || 'token',
      ...data,
    };

    if (this.writeStream) {
      this.writeStream.write(JSON.stringify(entry) + '\n');
    }

    // Console output in development
    if (process.env.NODE_ENV !== 'production' || process.env.DEBUG === 'true') {
      const prefix = `[TRACE][${entry.marketplace}][${stage}]`;
      const elapsed = entry.elapsedMs !== undefined ? `+${entry.elapsedMs}ms` : '';
      const msg = `${prefix} ${traceId} ${elapsed} ${data.decision || ''}`.trim();

      switch (level) {
        case 'error':
          warnLog('BidTraceLogger', `${msg} ${data.error?.message || ''}`);
          break;
        case 'warn':
          warnLog('BidTraceLogger', msg);
          break;
        case 'debug':
        case 'trace':
          if (process.env.DEBUG === 'true') console.debug(msg);
          break;
        default:
          console.log(msg);
      }
    }
  }

  // ============= Convenience Helper Methods =============

  /**
   * Log a deduplication check result
   */
  logDedupCheck(
    traceId: string,
    cacheType: 'orderhash' | 'pending',
    key: string,
    passed: boolean,
    data: Partial<BidTraceEntry> & { reason?: string }
  ): void {
    const stage: BidTraceStage = passed
      ? cacheType === 'orderhash'
        ? 'dedup_orderhash_passed'
        : 'dedup_pending_passed'
      : cacheType === 'orderhash'
        ? 'dedup_orderhash_blocked'
        : 'dedup_pending_blocked';

    this.log(traceId, stage, passed ? 'debug' : 'warn', {
      ...data,
      dedupKey: key,
      decision: passed
        ? `${cacheType} dedup PASSED`
        : `${cacheType} dedup BLOCKED: ${data.reason || 'already exists'}`,
    });
  }

  /**
   * Log when a job is queued
   */
  logJobQueued(
    traceId: string,
    jobId: string,
    jobName: string,
    data: Partial<BidTraceEntry> & { queueDepth?: number }
  ): void {
    this.log(traceId, 'job_queued', 'info', {
      ...data,
      jobId,
      jobName,
      decision: `Job ${jobName} queued`,
    });
  }

  /**
   * Log job started
   */
  logJobStarted(traceId: string, jobId: string, jobName: string, data: Partial<BidTraceEntry>): void {
    this.log(traceId, 'job_started', 'info', {
      ...data,
      jobId,
      jobName,
      decision: `Processing ${jobName}`,
    });
  }

  /**
   * Log bid API result
   */
  logBidResult(
    traceId: string,
    success: boolean,
    data: Partial<BidTraceEntry> & { ourOrderHash?: string; errorMessage?: string }
  ): void {
    if (success) {
      this.log(traceId, 'bid_api_success', 'info', {
        ...data,
        ourOrderHash: data.ourOrderHash,
        decision: `Bid placed successfully`,
      });
    } else {
      this.log(traceId, 'bid_api_error', 'error', {
        ...data,
        error: { message: data.errorMessage || 'Unknown error' },
        decision: `Bid failed: ${data.errorMessage}`,
      });
    }
  }

  /**
   * Log job completion
   */
  logJobCompleted(traceId: string, success: boolean, data: Partial<BidTraceEntry>): void {
    this.log(traceId, success ? 'job_completed' : 'job_failed', success ? 'info' : 'error', {
      ...data,
      decision: success ? 'Job completed successfully' : 'Job failed',
    });
  }

  /**
   * Log early return with reason
   */
  logEarlyReturn(traceId: string, reason: string, data: Partial<BidTraceEntry>): void {
    this.log(traceId, 'early_return', 'warn', {
      ...data,
      decision: `Early return: ${reason}`,
    });
  }

  /**
   * Log top bid check
   */
  logTopBidCheck(
    traceId: string,
    shouldProceed: boolean,
    data: Partial<BidTraceEntry> & {
      ourCurrentBid?: string;
      incomingBid?: string;
      reason?: string;
    }
  ): void {
    this.log(traceId, shouldProceed ? 'top_bid_check' : 'top_bid_skip', shouldProceed ? 'debug' : 'warn', {
      ...data,
      existingBidAmount: data.ourCurrentBid,
      incomingBidAmount: data.incomingBid,
      decision: shouldProceed
        ? 'Proceeding with counterbid'
        : `Skipping: ${data.reason || 'would not become top bidder'}`,
    });
  }

  /**
   * Clean up trace context after completion
   */
  completeTrace(traceId: string): void {
    this.traceContexts.delete(traceId);
  }

  // ============= Query Methods =============

  /**
   * Get traces matching filter criteria
   */
  async getTraces(filter: BidTraceFilter = {}): Promise<BidTraceQueryResult> {
    const traces: BidTraceEntry[] = [];
    const now = new Date();
    let { startTime, endTime } = filter;
    endTime = endTime || now;

    // Default to last hour if no time specified
    if (!startTime) {
      startTime = new Date(now.getTime() - 60 * 60 * 1000);
    }

    // Enforce max 1 day range
    const maxTimeRange = 24 * 60 * 60 * 1000;
    if (endTime.getTime() - startTime.getTime() > maxTimeRange) {
      startTime = new Date(endTime.getTime() - maxTimeRange);
    }

    try {
      if (!fs.existsSync(this.logsDir)) {
        return this.buildQueryResult([], filter);
      }

      const files = fs
        .readdirSync(this.logsDir)
        .filter((f) => f.startsWith('bid-traces-') && f.endsWith('.log'))
        .sort();

      for (const file of files) {
        const dateMatch = file.match(/(\d{4}-\d{2}-\d{2})\.log$/);
        if (dateMatch && startTime) {
          const fileDate = new Date(dateMatch[1] + 'T00:00:00Z');
          if (fileDate > new Date(endTime.getTime() + 24 * 60 * 60 * 1000)) continue;
          if (fileDate < new Date(startTime.getTime() - 24 * 60 * 60 * 1000)) continue;
        }

        try {
          const filePath = path.join(this.logsDir, file);
          const stats = fs.statSync(filePath);
          if (stats.size > 100 * 1024 * 1024) continue; // Skip > 100MB files

          const contents = fs.readFileSync(filePath, 'utf-8');
          const lines = contents.split('\n');

          for (const line of lines) {
            if (!line.trim()) continue;
            try {
              const entry = JSON.parse(line) as BidTraceEntry;
              if (this.matchesFilter(entry, filter, startTime, endTime)) {
                traces.push(entry);
                if (traces.length > 50000) traces.shift(); // Sliding window
              }
            } catch {
              // Skip malformed entries
            }
          }
        } catch {
          // Skip files with read errors
        }
      }
    } catch (err) {
      warnLog('BidTraceLogger', `Error in getTraces: ${err}`);
    }

    return this.buildQueryResult(
      traces.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()),
      filter
    );
  }

  private matchesFilter(
    entry: BidTraceEntry,
    filter: BidTraceFilter,
    startTime?: Date,
    endTime?: Date
  ): boolean {
    const t = new Date(entry.timestamp);
    if (isNaN(t.getTime())) return false;
    if (startTime && t < startTime) return false;
    if (endTime && t > endTime) return false;
    if (filter.traceId && entry.traceId !== filter.traceId) return false;
    if (filter.correlationId && entry.correlationId !== filter.correlationId) return false;
    if (filter.taskId && entry.taskId !== filter.taskId) return false;
    if (filter.tokenId && entry.tokenId !== filter.tokenId) return false;
    if (filter.marketplace && entry.marketplace !== filter.marketplace) return false;
    if (filter.orderHash && entry.orderHash !== filter.orderHash) return false;
    if (filter.collectionSlug && entry.collectionSlug !== filter.collectionSlug) return false;
    if (filter.bidType && entry.bidType !== filter.bidType) return false;

    if (filter.stage) {
      const stages = Array.isArray(filter.stage) ? filter.stage : [filter.stage];
      if (!stages.includes(entry.stage)) return false;
    }

    if (filter.level) {
      const levels = Array.isArray(filter.level) ? filter.level : [filter.level];
      if (!levels.includes(entry.level)) return false;
    }

    if (filter.hasError !== undefined) {
      const hasError = !!entry.error;
      if (filter.hasError !== hasError) return false;
    }

    return true;
  }

  private buildQueryResult(traces: BidTraceEntry[], _filter: BidTraceFilter): BidTraceQueryResult {
    const byLevel: Record<BidTraceLevel, number> = { trace: 0, debug: 0, info: 0, warn: 0, error: 0 };
    const byStage: Record<string, number> = {};
    const byOutcome = { success: 0, rejected: 0, failed: 0, pending: 0 };

    for (const t of traces) {
      byLevel[t.level]++;
      byStage[t.stage] = (byStage[t.stage] || 0) + 1;

      if (t.stage === 'job_completed' || t.stage === 'bid_api_success') byOutcome.success++;
      else if (t.stage === 'job_failed' || t.stage === 'bid_api_error') byOutcome.failed++;
      else if (
        t.stage === 'dedup_orderhash_blocked' ||
        t.stage === 'dedup_pending_blocked' ||
        t.stage === 'top_bid_skip' ||
        t.stage === 'early_return'
      )
        byOutcome.rejected++;
    }

    return {
      traces,
      summary: { total: traces.length, byLevel, byStage, byOutcome },
      pagination: {
        total: traces.length,
        limit: 50000,
        offset: 0,
        hasMore: traces.length >= 50000,
      },
    };
  }

  /**
   * Find potential duplicate bids for a specific token
   */
  async findDuplicates(
    taskId: string,
    tokenId: string,
    timeWindowMs: number = 60000
  ): Promise<BidTraceDuplicateGroup[]> {
    const result = await this.getTraces({
      taskId,
      tokenId,
      stage: ['bid_api_success', 'job_completed'],
    });

    // Group by correlation ID within time window
    const groups: Map<string, BidTraceEntry[]> = new Map();

    for (const trace of result.traces) {
      const key = trace.correlationId;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(trace);
    }

    // Find groups with multiple successful bids
    const duplicates: BidTraceDuplicateGroup[] = [];

    for (const [correlationId, traces] of groups.entries()) {
      const successfulBids = traces.filter(
        (t) => t.stage === 'bid_api_success' || t.stage === 'job_completed'
      );

      if (successfulBids.length > 1) {
        const times = successfulBids.map((t) => new Date(t.timestamp).getTime());
        const firstTime = Math.min(...times);
        const lastTime = Math.max(...times);

        if (lastTime - firstTime <= timeWindowMs) {
          duplicates.push({
            correlationId,
            tokenId,
            taskId,
            marketplace: traces[0]?.marketplace || 'opensea',
            timeWindowMs: lastTime - firstTime,
            traces: successfulBids,
            successfulBids: successfulBids.length,
            firstBidTime: new Date(firstTime).toISOString(),
            lastBidTime: new Date(lastTime).toISOString(),
          });
        }
      }
    }

    return duplicates;
  }

  /**
   * Get a summary of a specific trace
   */
  async getTraceSummary(traceId: string): Promise<BidTraceSummary | null> {
    const result = await this.getTraces({ traceId });
    if (result.traces.length === 0) return null;

    const traces = result.traces.sort(
      (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );

    const first = traces[0];
    const last = traces[traces.length - 1];
    const stages = traces.map((t) => t.stage);

    let outcome: 'success' | 'rejected' | 'failed' | 'pending' = 'pending';
    let rejectReason: string | undefined;
    let errorMessage: string | undefined;

    if (stages.includes('bid_api_success') || stages.includes('job_completed')) {
      outcome = 'success';
    } else if (stages.includes('bid_api_error') || stages.includes('job_failed')) {
      outcome = 'failed';
      const errorTrace = traces.find((t) => t.error);
      errorMessage = errorTrace?.error?.message;
    } else if (
      stages.includes('dedup_orderhash_blocked') ||
      stages.includes('dedup_pending_blocked') ||
      stages.includes('top_bid_skip') ||
      stages.includes('early_return')
    ) {
      outcome = 'rejected';
      const rejectTrace = traces.find(
        (t) =>
          t.stage === 'dedup_orderhash_blocked' ||
          t.stage === 'dedup_pending_blocked' ||
          t.stage === 'top_bid_skip' ||
          t.stage === 'early_return'
      );
      rejectReason = rejectTrace?.decision;
    }

    const successTrace = traces.find((t) => t.stage === 'bid_api_success');

    return {
      traceId,
      correlationId: first.correlationId,
      taskId: first.taskId,
      tokenId: first.tokenId,
      traitKey: first.traitKey,
      marketplace: first.marketplace,
      bidType: first.bidType,
      collectionSlug: first.collectionSlug,
      startTime: first.timestamp,
      endTime: last.timestamp,
      durationMs: new Date(last.timestamp).getTime() - new Date(first.timestamp).getTime(),
      outcome,
      rejectReason,
      errorMessage,
      stages,
      ourBidAmount: successTrace?.ourBidAmount,
    };
  }

  /**
   * Export traces to JSON or CSV
   */
  async exportTraces(
    filter: BidTraceFilter & { format: 'json' | 'csv' }
  ): Promise<Buffer> {
    const result = await this.getTraces(filter);

    if (filter.format === 'json') {
      return Buffer.from(JSON.stringify(result.traces, null, 2));
    }

    // CSV export
    const headers = [
      'timestamp',
      'traceId',
      'correlationId',
      'elapsedMs',
      'stage',
      'level',
      'marketplace',
      'taskId',
      'tokenId',
      'collectionSlug',
      'bidType',
      'orderHash',
      'incomingBidAmount',
      'ourBidAmount',
      'decision',
      'error',
    ];

    const lines: string[] = [headers.join(',')];

    for (const t of result.traces) {
      const row = [
        this.escapeCsv(t.timestamp),
        this.escapeCsv(t.traceId),
        this.escapeCsv(t.correlationId),
        t.elapsedMs?.toString() || '',
        this.escapeCsv(t.stage),
        this.escapeCsv(t.level),
        this.escapeCsv(t.marketplace),
        this.escapeCsv(t.taskId),
        this.escapeCsv(t.tokenId || ''),
        this.escapeCsv(t.collectionSlug),
        this.escapeCsv(t.bidType),
        this.escapeCsv(t.orderHash || ''),
        this.escapeCsv(t.incomingBidAmount || ''),
        this.escapeCsv(t.ourBidAmount || ''),
        this.escapeCsv(t.decision || ''),
        this.escapeCsv(t.error?.message || ''),
      ];
      lines.push(row.join(','));
    }

    const bom = '\uFEFF';
    return Buffer.from(bom + lines.join('\n'), 'utf8');
  }

  private escapeCsv(field: string): string {
    const str = field != null ? String(field) : '';
    if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
  }

  destroy() {
    if (this.writeStream) this.writeStream.end();
    if (this.rotationTimer) clearInterval(this.rotationTimer);
    if (this.contextCleanupTimer) clearInterval(this.contextCleanupTimer);
    this.traceContexts.clear();
  }
}

// Singleton instance
export const bidTraceLogger = new BidTraceLogger();

// Re-export types for convenience
export type {
  BidTraceEntry,
  BidTraceFilter,
  BidTraceLevel,
  BidTraceStage,
  BidTraceMarketplace,
  BidTraceBidType,
  BidTraceQueryResult,
  BidTraceDuplicateGroup,
  BidTraceSummary,
  BidTraceContext,
};
