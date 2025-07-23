import { Job } from 'bullmq';
import { logger } from './logger';

export enum FailureCategory {
  LOCK_RENEWAL = 'LOCK_RENEWAL',
  MEMORY_PRESSURE = 'MEMORY_PRESSURE',
  RATE_LIMIT = 'RATE_LIMIT',
  NETWORK_TIMEOUT = 'NETWORK_TIMEOUT',
  INVALID_DATA = 'INVALID_DATA',
  DEPENDENCY_FAILURE = 'DEPENDENCY_FAILURE',
  UNKNOWN = 'UNKNOWN',
}

export interface FailureAnalysis {
  category: FailureCategory;
  isRetryable: boolean;
  suggestedDelay: number;
  details: string;
  recommendations: string[];
}

export class JobFailureAnalyzer {
  private static failurePatterns = [
    {
      pattern: /could not renew lock/i,
      category: FailureCategory.LOCK_RENEWAL,
      isRetryable: true,
      suggestedDelay: 5000,
      recommendations: [
        'Increase lockDuration in worker options',
        'Reduce job processing time',
        'Check if job is taking longer than expected',
      ],
    },
    {
      pattern: /rate limit|429|too many requests/i,
      category: FailureCategory.RATE_LIMIT,
      isRetryable: true,
      suggestedDelay: 30000,
      recommendations: [
        'Implement exponential backoff',
        'Reduce concurrency',
        'Add rate limit tracking',
      ],
    },
    {
      pattern: /ENOMEM|out of memory|heap out of memory/i,
      category: FailureCategory.MEMORY_PRESSURE,
      isRetryable: false,
      suggestedDelay: 60000,
      recommendations: [
        'Reduce worker concurrency',
        'Implement job batching',
        'Increase system memory',
        'Enable memory monitoring',
      ],
    },
    {
      pattern: /ETIMEDOUT|ESOCKETTIMEDOUT|timeout|ECONNREFUSED/i,
      category: FailureCategory.NETWORK_TIMEOUT,
      isRetryable: true,
      suggestedDelay: 10000,
      recommendations: [
        'Increase timeout values',
        'Check network connectivity',
        'Implement retry with backoff',
      ],
    },
    {
      pattern: /invalid|undefined|null|cannot read property/i,
      category: FailureCategory.INVALID_DATA,
      isRetryable: false,
      suggestedDelay: 0,
      recommendations: [
        'Add data validation',
        'Check job data structure',
        'Implement error boundaries',
      ],
    },
    {
      pattern: /mongo|redis|database|connection/i,
      category: FailureCategory.DEPENDENCY_FAILURE,
      isRetryable: true,
      suggestedDelay: 15000,
      recommendations: [
        'Check database connectivity',
        'Implement connection pooling',
        'Add health checks',
      ],
    },
  ];

  static analyzeFailure(job: Job, error: Error | string): FailureAnalysis {
    const errorMessage = error instanceof Error ? error.message : String(error);
    const errorStack = error instanceof Error ? error.stack : '';
    
    // Find matching pattern
    for (const pattern of this.failurePatterns) {
      if (pattern.pattern.test(errorMessage) || pattern.pattern.test(errorStack || '')) {
        return {
          category: pattern.category,
          isRetryable: pattern.isRetryable,
          suggestedDelay: pattern.suggestedDelay,
          details: errorMessage,
          recommendations: pattern.recommendations,
        };
      }
    }
    
    // Default unknown failure
    return {
      category: FailureCategory.UNKNOWN,
      isRetryable: true,
      suggestedDelay: 5000,
      details: errorMessage,
      recommendations: ['Review error logs', 'Add specific error handling'],
    };
  }

  static async logFailureAnalysis(job: Job, error: Error | string): Promise<FailureAnalysis> {
    const analysis = this.analyzeFailure(job, error);
    
    await logger.logJobFailure('Job failed', {
      jobId: job.id,
      jobName: job.name,
      attemptNumber: job.attemptsMade,
      failureCategory: analysis.category,
      isRetryable: analysis.isRetryable,
      errorMessage: analysis.details,
      recommendations: analysis.recommendations,
      jobData: job.data,
    });
    
    return analysis;
  }

  static shouldRetry(analysis: FailureAnalysis, attemptsMade: number, maxAttempts: number): boolean {
    // Never retry non-retryable failures
    if (!analysis.isRetryable) return false;
    
    // Check attempt limit
    if (maxAttempts > 0 && attemptsMade >= maxAttempts) return false;
    
    // Special handling for specific categories
    switch (analysis.category) {
      case FailureCategory.MEMORY_PRESSURE:
        // Only retry memory issues after GC and delay
        return attemptsMade < 2;
        
      case FailureCategory.RATE_LIMIT:
        // Always retry rate limits with appropriate delay
        return true;
        
      case FailureCategory.LOCK_RENEWAL:
        // Retry lock renewals up to 3 times
        return attemptsMade < 3;
        
      default:
        return analysis.isRetryable;
    }
  }

  static getRetryDelay(analysis: FailureAnalysis, attemptsMade: number): number {
    const baseDelay = analysis.suggestedDelay;
    
    // Exponential backoff for rate limits
    if (analysis.category === FailureCategory.RATE_LIMIT) {
      return Math.min(baseDelay * Math.pow(2, attemptsMade), 300000); // Max 5 minutes
    }
    
    // Linear increase for other retryable failures
    return Math.min(baseDelay * (attemptsMade + 1), 60000); // Max 1 minute
  }
}

// Circuit breaker for preventing cascading failures
export class JobCircuitBreaker {
  private failures = new Map<string, number>();
  private lastFailTime = new Map<string, number>();
  private state = new Map<string, 'CLOSED' | 'OPEN' | 'HALF_OPEN'>();
  
  constructor(
    private failureThreshold = 5,
    private resetTimeout = 60000,
    private halfOpenTests = 1
  ) {}
  
  async execute<T>(
    key: string,
    fn: () => Promise<T>,
    fallback?: () => T
  ): Promise<T> {
    const currentState = this.state.get(key) || 'CLOSED';
    
    if (currentState === 'OPEN') {
      const lastFail = this.lastFailTime.get(key) || 0;
      if (Date.now() - lastFail > this.resetTimeout) {
        this.state.set(key, 'HALF_OPEN');
      } else {
        if (fallback) return fallback();
        throw new Error(`Circuit breaker is OPEN for ${key}`);
      }
    }
    
    try {
      const result = await fn();
      
      if (currentState === 'HALF_OPEN') {
        this.state.set(key, 'CLOSED');
        this.failures.set(key, 0);
      }
      
      return result;
    } catch (error) {
      const failCount = (this.failures.get(key) || 0) + 1;
      this.failures.set(key, failCount);
      this.lastFailTime.set(key, Date.now());
      
      if (failCount >= this.failureThreshold) {
        this.state.set(key, 'OPEN');
        await logger.logGeneralError(`Circuit breaker opened for ${key}`, {
          failures: failCount,
          threshold: this.failureThreshold,
        });
      }
      
      throw error;
    }
  }
  
  getState(key: string): string {
    return this.state.get(key) || 'CLOSED';
  }
  
  reset(key: string): void {
    this.state.delete(key);
    this.failures.delete(key);
    this.lastFailTime.delete(key);
  }
}

// Job retry strategy
export function createRetryStrategy(job: Job, error: Error | string) {
  const analysis = JobFailureAnalyzer.analyzeFailure(job, error);
  
  return {
    shouldRetry: JobFailureAnalyzer.shouldRetry(
      analysis,
      job.attemptsMade,
      job.opts.attempts || 3
    ),
    delay: JobFailureAnalyzer.getRetryDelay(analysis, job.attemptsMade),
    analysis,
  };
}