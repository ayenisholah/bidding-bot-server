import { logger } from './logger';

interface TaskFailureRecord {
  taskId: string;
  collection: string;
  marketplace: string;
  consecutiveFailures: number;
  lastFailure: number;
  totalTimeouts: number;
  suspended: boolean;
  suspendedUntil?: number;
}

export class TaskCircuitBreaker {
  private failureRecords: Map<string, TaskFailureRecord> = new Map();
  private readonly maxConsecutiveFailures = 5;
  private readonly suspensionDurationMs = 300000; // 5 minutes
  private readonly resetWindowMs = 600000; // 10 minutes

  private getKey(taskId: string, marketplace: string): string {
    return `${taskId}:${marketplace}`;
  }

  recordTimeout(taskId: string, collection: string, marketplace: string): boolean {
    const key = this.getKey(taskId, marketplace);
    const now = Date.now();
    
    let record = this.failureRecords.get(key);
    
    if (!record) {
      record = {
        taskId,
        collection,
        marketplace,
        consecutiveFailures: 0,
        lastFailure: now,
        totalTimeouts: 0,
        suspended: false
      };
      this.failureRecords.set(key, record);
    }

    // Reset if last failure was too long ago
    if (now - record.lastFailure > this.resetWindowMs) {
      record.consecutiveFailures = 0;
      record.totalTimeouts = 0;
    }

    record.consecutiveFailures++;
    record.totalTimeouts++;
    record.lastFailure = now;

    // Check if we should suspend
    if (record.consecutiveFailures >= this.maxConsecutiveFailures) {
      record.suspended = true;
      record.suspendedUntil = now + this.suspensionDurationMs;
      
      console.log(`[Circuit Breaker] Suspending ${marketplace} for task ${taskId} (${collection}) due to ${record.consecutiveFailures} consecutive timeouts`);
      
      logger.logGeneralError('Task suspended due to repeated timeouts', {
        taskId,
        collection,
        marketplace,
        consecutiveFailures: record.consecutiveFailures,
        totalTimeouts: record.totalTimeouts,
        suspendedUntil: new Date(record.suspendedUntil).toISOString()
      });
      
      return true; // Task should be suspended
    }

    return false;
  }

  recordSuccess(taskId: string, marketplace: string): void {
    const key = this.getKey(taskId, marketplace);
    const record = this.failureRecords.get(key);
    
    if (record) {
      record.consecutiveFailures = 0;
      // Don't reset totalTimeouts to maintain history
    }
  }

  isSuspended(taskId: string, marketplace: string): boolean {
    const key = this.getKey(taskId, marketplace);
    const record = this.failureRecords.get(key);
    
    if (!record || !record.suspended) {
      return false;
    }

    const now = Date.now();
    
    // Check if suspension period has expired
    if (record.suspendedUntil && now >= record.suspendedUntil) {
      record.suspended = false;
      record.consecutiveFailures = 0;
      delete record.suspendedUntil;
      
      console.log(`[Circuit Breaker] Resuming ${marketplace} for task ${taskId} after suspension period`);
      return false;
    }

    return true;
  }

  getStatus(): Array<{
    taskId: string;
    collection: string;
    marketplace: string;
    status: string;
    consecutiveFailures: number;
    totalTimeouts: number;
    suspendedUntil?: string;
  }> {
    const status = [];
    const now = Date.now();

    for (const [key, record] of this.failureRecords.entries()) {
      const isSuspended = record.suspended && record.suspendedUntil && now < record.suspendedUntil;
      
      status.push({
        taskId: record.taskId,
        collection: record.collection,
        marketplace: record.marketplace,
        status: isSuspended ? 'SUSPENDED' : 'ACTIVE',
        consecutiveFailures: record.consecutiveFailures,
        totalTimeouts: record.totalTimeouts,
        suspendedUntil: record.suspendedUntil ? new Date(record.suspendedUntil).toISOString() : undefined
      });
    }

    return status;
  }

  clearTask(taskId: string): void {
    // Remove all records for a specific task
    const keysToDelete = [];
    for (const key of this.failureRecords.keys()) {
      if (key.startsWith(`${taskId}:`)) {
        keysToDelete.push(key);
      }
    }
    
    keysToDelete.forEach(key => this.failureRecords.delete(key));
  }
}

// Export singleton instance
export const taskCircuitBreaker = new TaskCircuitBreaker();