import { errorLog } from './debug-file-logger';

interface BullMQError {
  message: string;
  stack?: string;
  jobId?: string;
  jobName?: string;
  timestamp?: Date;
}

interface FormattedError {
  type: string;
  jobId: string;
  jobName: string;
  timestamp: string;
  details: string;
}

/**
 * Formats BullMQ lock renewal errors for better readability
 */
export class BullMQErrorFormatter {
  private static lockErrorPattern = /Error: could not renew lock for job (.+)/;
  private static errorBuffer: FormattedError[] = [];
  private static flushInterval: NodeJS.Timeout | null = null;
  private static readonly BUFFER_SIZE = 10;
  private static readonly FLUSH_INTERVAL = 5000; // 5 seconds

  /**
   * Initialize the error formatter with automatic flushing
   */
  static initialize() {
    if (!this.flushInterval) {
      this.flushInterval = setInterval(() => {
        this.flushErrors();
      }, this.FLUSH_INTERVAL);

      // Flush on process exit
      process.on('beforeExit', () => this.flushErrors());
      process.on('SIGINT', () => {
        this.flushErrors();
        process.exit(0);
      });
      process.on('SIGTERM', () => {
        this.flushErrors();
        process.exit(0);
      });
    }
  }

  /**
   * Format and log a BullMQ error
   */
  static async formatError(error: Error | BullMQError, context?: any) {
    const match = error.message.match(this.lockErrorPattern);

    if (match) {
      const jobId = match[1];
      const formatted: FormattedError = {
        type: 'LOCK_RENEWAL_FAILED',
        jobId: jobId,
        jobName: this.extractJobName(jobId),
        timestamp: new Date().toISOString(),
        details: this.extractDetails(jobId)
      };

      this.errorBuffer.push(formatted);

      // Flush if buffer is full
      if (this.errorBuffer.length >= this.BUFFER_SIZE) {
        await this.flushErrors();
      }
    } else {
      // Handle other BullMQ errors
      errorLog('[Queue]', error.message, error as Error);
    }
  }

  /**
   * Extract job name from job ID
   */
  private static extractJobName(jobId: string): string {
    // Handle different job ID formats
    if (jobId.includes('-')) {
      const parts = jobId.split('-');
      if (parts.length >= 3) {
        // Format: taskId-marketplace-trait-value-collection
        return `${parts[1]} ${parts.slice(2).join(' ')}`;
      }
    }

    // Numeric job IDs
    if (/^\d+$/.test(jobId)) {
      return `Job #${jobId}`;
    }

    return jobId;
  }

  /**
   * Extract additional details from job ID
   */
  private static extractDetails(jobId: string): string {
    if (jobId.includes('-')) {
      const parts = jobId.split('-');
      if (parts.length >= 3) {
        const marketplace = parts[1];
        const trait = parts[2];
        const value = parts[3];
        const collection = parts.slice(4).join('-');

        return `Marketplace: ${marketplace}, Trait: ${trait}=${value}, Collection: ${collection}`;
      }
    }

    return 'Standard job processing';
  }

  /**
   * Flush accumulated errors to logger
   */
  private static async flushErrors() {
    if (this.errorBuffer.length === 0) return;

    const errors = [...this.errorBuffer];
    this.errorBuffer = [];

    // Group errors by type
    const groupedErrors = errors.reduce((acc, error) => {
      if (!acc[error.type]) {
        acc[error.type] = [];
      }
      acc[error.type].push(error);
      return acc;
    }, {} as Record<string, FormattedError[]>);

    // Log grouped errors
    for (const [type, typeErrors] of Object.entries(groupedErrors)) {
      const summary = this.createErrorSummary(type, typeErrors);
      errorLog('[Queue]', `${summary} - count=${typeErrors.length} jobs=${typeErrors.map(e => e.jobId).join(',')}`);
    }
  }

  /**
   * Create a summary of errors
   */
  private static createErrorSummary(type: string, errors: FormattedError[]): string {
    const jobSummary = errors.slice(0, 3).map(e => e.jobName).join(', ');
    const moreCount = errors.length > 3 ? ` and ${errors.length - 3} more` : '';

    return `${type}: ${errors.length} jobs affected (${jobSummary}${moreCount})`;
  }

  /**
   * Print formatted errors to console
   */
  static printFormattedErrors() {
    if (this.errorBuffer.length === 0) {
      // console.log('No BullMQ errors in buffer');
      return;
    }

    // console.log('\n=== BullMQ Lock Renewal Errors ===');
    // console.log(`Total errors: ${this.errorBuffer.length}\n`);

    this.errorBuffer.forEach((error, index) => {
      // console.log(`[${index + 1}] ${error.type}`);
      // console.log(`    Job: ${error.jobName}`);
      // console.log(`    ID: ${error.jobId}`);
      // console.log(`    Time: ${error.timestamp}`);
      // console.log(`    ${error.details}\n`);
    });
  }

  /**
   * Get current error statistics
   */
  static getStats() {
    return {
      bufferedErrors: this.errorBuffer.length,
      errorTypes: [...new Set(this.errorBuffer.map(e => e.type))],
      affectedJobs: [...new Set(this.errorBuffer.map(e => e.jobName))]
    };
  }

  /**
   * Clear the error buffer
   */
  static clear() {
    this.errorBuffer = [];
  }

  /**
   * Stop the formatter and flush remaining errors
   */
  static async stop() {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
    }
    await this.flushErrors();
  }
}

// Initialize on module load
BullMQErrorFormatter.initialize();