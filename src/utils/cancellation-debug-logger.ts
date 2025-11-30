import * as fs from 'fs';
import * as path from 'path';
import { warnLog } from './debug-file-logger';

const DEBUG = process.env.DEBUG === 'true';

// Log directory in project root (one level up from bidding-bot-server)
const LOG_DIR = path.resolve(process.cwd(), '..', 'cancellation-debug-logs');

interface CancellationLogEntry {
  elapsed: string;
  phase: string;
  message: string;
  data?: Record<string, unknown>;
}

class CancellationDebugLogger {
  private logFile: string | null = null;
  private entries: CancellationLogEntry[] = [];
  private startTime: number = 0;
  private taskId: string = '';
  private slug: string = '';
  private isActive: boolean = false;

  // Job result counters
  private successCount: number = 0;
  private errorCount: number = 0;
  private skippedCount: number = 0;

  /**
   * Start a new cancellation debug session
   * Only activates if DEBUG=true
   * Overwrites existing log file for the same taskId
   */
  start(taskId: string, slug: string): void {
    if (!DEBUG) return;

    this.taskId = taskId;
    this.slug = slug;
    this.startTime = Date.now();
    this.entries = [];
    this.isActive = true;

    // Reset counters
    this.successCount = 0;
    this.errorCount = 0;
    this.skippedCount = 0;

    // Ensure log directory exists
    if (!fs.existsSync(LOG_DIR)) {
      fs.mkdirSync(LOG_DIR, { recursive: true });
    }

    // Fixed filename per taskId - overwrites previous logs
    this.logFile = path.join(LOG_DIR, `cancellation-debug-${taskId}.log`);
  }

  /**
   * Log an entry with phase, message, and optional data
   */
  log(phase: string, message: string, data?: Record<string, unknown>): void {
    if (!DEBUG || !this.isActive) return;

    const elapsed = this.formatElapsed(Date.now() - this.startTime);
    this.entries.push({ elapsed, phase, message, data });
  }

  /**
   * Log a cancellation job result and update counters
   */
  logJobResult(orderId: string, result: 'success' | 'error' | 'skipped', reason?: string): void {
    if (!DEBUG || !this.isActive) return;

    const shortOrderId = orderId.substring(0, 16) + '...';
    const elapsed = this.formatElapsed(Date.now() - this.startTime);

    switch (result) {
      case 'success':
        this.successCount++;
        this.entries.push({ elapsed, phase: 'CANCEL_JOB', message: `SUCCESS | orderId: ${shortOrderId}` });
        break;
      case 'error':
        this.errorCount++;
        this.entries.push({ elapsed, phase: 'CANCEL_JOB', message: `ERROR | orderId: ${shortOrderId} | reason: ${reason || 'unknown'}` });
        break;
      case 'skipped':
        this.skippedCount++;
        this.entries.push({ elapsed, phase: 'CANCEL_JOB', message: `SKIPPED | orderId: ${shortOrderId} | reason: ${reason || 'unknown'}` });
        break;
    }
  }

  /**
   * Get the current job result counts
   */
  getJobCounts(): { success: number; error: number; skipped: number } {
    return {
      success: this.successCount,
      error: this.errorCount,
      skipped: this.skippedCount
    };
  }

  /**
   * Finish the debug session and write all entries to file
   */
  finish(summary: Record<string, unknown>): void {
    if (!DEBUG || !this.isActive || !this.logFile) return;

    const duration = ((Date.now() - this.startTime) / 1000).toFixed(2);

    // Build log content (no timestamp - file is overwritten each run)
    let content = '';
    content += '================================================================================\n';
    content += 'CANCELLATION DEBUG LOG\n';
    content += `Task: ${this.taskId} | Slug: ${this.slug}\n`;
    content += '================================================================================\n\n';

    // Write all entries
    for (const entry of this.entries) {
      content += `[${entry.elapsed}] [${entry.phase}] ${entry.message}\n`;
      if (entry.data) {
        const dataStr = this.formatData(entry.data);
        content += `            ${dataStr}\n`;
      }
    }

    // Write summary
    content += '\n================================================================================\n';
    content += 'SUMMARY\n';
    content += `Duration: ${duration}s\n`;
    for (const [key, value] of Object.entries(summary)) {
      content += `${key}: ${JSON.stringify(value)}\n`;
    }
    content += '================================================================================\n';

    // Write to file
    try {
      fs.writeFileSync(this.logFile, content, 'utf8');
      console.log(`[CancellationDebug] Log written to: ${this.logFile}`);
    } catch (err) {
      warnLog('CancellationDebug', `Failed to write log file: ${err}`);
    }

    // Reset state
    this.isActive = false;
    this.entries = [];
    this.logFile = null;
  }

  /**
   * Format elapsed time as MM:SS.mmm
   */
  private formatElapsed(ms: number): string {
    const totalSeconds = Math.floor(ms / 1000);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    const millis = ms % 1000;
    return `${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}.${millis.toString().padStart(3, '0')}`;
  }

  /**
   * Format data object for display
   */
  private formatData(data: Record<string, unknown>): string {
    return Object.entries(data)
      .map(([key, value]) => {
        if (Array.isArray(value)) {
          if (value.length <= 5) {
            return `${key}: [${value.join(', ')}]`;
          }
          return `${key}: [${value.slice(0, 3).join(', ')}... +${value.length - 3} more]`;
        }
        return `${key}: ${value}`;
      })
      .join(' | ');
  }
}

export const cancellationDebugLogger = new CancellationDebugLogger();
