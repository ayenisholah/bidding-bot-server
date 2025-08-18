import { logger, ErrorCategory } from './logger';

export enum CircuitState {
  CLOSED = 'CLOSED',     // Normal operation
  OPEN = 'OPEN',         // Failing, reject all requests
  HALF_OPEN = 'HALF_OPEN' // Testing if service recovered
}

export interface CircuitBreakerOptions {
  failureThreshold: number;      // Number of failures before opening circuit
  successThreshold: number;      // Number of successes in half-open before closing
  timeout: number;              // Time in ms before trying half-open
  volumeThreshold: number;      // Minimum requests before evaluating
  errorThresholdPercentage: number; // Error percentage to open circuit
}

export class CircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED;
  private failureCount: number = 0;
  private successCount: number = 0;
  private lastFailureTime: number = 0;
  private requestCount: number = 0;
  private errorCount: number = 0;
  private lastResetTime: number = Date.now();
  private halfOpenTestInProgress: boolean = false;

  constructor(
    private name: string,
    private options: CircuitBreakerOptions = {
      failureThreshold: 5,
      successThreshold: 3,
      timeout: 60000, // 1 minute
      volumeThreshold: 10,
      errorThresholdPercentage: 50
    }
  ) {}

  async execute<T>(operation: () => Promise<T>): Promise<T | null> {
    // Check if circuit should transition from OPEN to HALF_OPEN
    if (this.state === CircuitState.OPEN) {
      const now = Date.now();
      if (now - this.lastFailureTime >= this.options.timeout) {
        console.log(`[CircuitBreaker ${this.name}] Transitioning from OPEN to HALF_OPEN`);
        this.state = CircuitState.HALF_OPEN;
        this.halfOpenTestInProgress = false;
      } else {
        // Circuit is OPEN, reject immediately
        console.warn(`[CircuitBreaker ${this.name}] Circuit is OPEN, rejecting request`);
        return null;
      }
    }

    // If HALF_OPEN and test in progress, reject additional requests
    if (this.state === CircuitState.HALF_OPEN && this.halfOpenTestInProgress) {
      console.warn(`[CircuitBreaker ${this.name}] Circuit is HALF_OPEN with test in progress, rejecting request`);
      return null;
    }

    try {
      // Mark test in progress for HALF_OPEN state
      if (this.state === CircuitState.HALF_OPEN) {
        this.halfOpenTestInProgress = true;
      }

      // Execute the operation
      const result = await operation();
      
      // Operation succeeded
      this.onSuccess();
      
      return result;
    } catch (error) {
      // Operation failed
      this.onFailure(error);
      
      // Re-throw for operations that need to handle the error
      throw error;
    } finally {
      if (this.state === CircuitState.HALF_OPEN) {
        this.halfOpenTestInProgress = false;
      }
    }
  }

  private onSuccess(): void {
    this.requestCount++;

    if (this.state === CircuitState.HALF_OPEN) {
      this.successCount++;
      console.log(`[CircuitBreaker ${this.name}] Success in HALF_OPEN state (${this.successCount}/${this.options.successThreshold})`);
      
      if (this.successCount >= this.options.successThreshold) {
        this.close();
      }
    } else if (this.state === CircuitState.CLOSED) {
      // Reset failure count on success in CLOSED state
      this.failureCount = 0;
    }
  }

  private onFailure(error: any): void {
    this.requestCount++;
    this.errorCount++;
    this.lastFailureTime = Date.now();

    // Log the failure
    logger.logWarning(ErrorCategory.DATABASE, `Circuit breaker ${this.name} recorded failure`, {
      state: this.state,
      error: error,
      errorMessage: error instanceof Error ? error.message : String(error),
      failureCount: this.failureCount + 1,
      errorRate: this.getErrorRate()
    });

    if (this.state === CircuitState.HALF_OPEN) {
      // Any failure in HALF_OPEN state opens the circuit
      console.error(`[CircuitBreaker ${this.name}] Failure in HALF_OPEN state, opening circuit`);
      this.open();
    } else if (this.state === CircuitState.CLOSED) {
      this.failureCount++;
      
      // Check if we should open the circuit
      if (this.shouldOpen()) {
        this.open();
      }
    }
  }

  private shouldOpen(): boolean {
    // Check failure threshold
    if (this.failureCount >= this.options.failureThreshold) {
      return true;
    }

    // Check error rate threshold
    if (this.requestCount >= this.options.volumeThreshold) {
      const errorRate = this.getErrorRate();
      if (errorRate >= this.options.errorThresholdPercentage) {
        return true;
      }
    }

    return false;
  }

  private getErrorRate(): number {
    if (this.requestCount === 0) return 0;
    return (this.errorCount / this.requestCount) * 100;
  }

  private open(): void {
    this.state = CircuitState.OPEN;
    this.successCount = 0;
    console.error(`[CircuitBreaker ${this.name}] Circuit OPENED - failures: ${this.failureCount}, error rate: ${this.getErrorRate().toFixed(2)}%`);
    
    logger.logError(ErrorCategory.DATABASE, `Circuit breaker ${this.name} opened`, {
      failureCount: this.failureCount,
      errorRate: this.getErrorRate(),
      lastFailureTime: this.lastFailureTime
    });
  }

  private close(): void {
    this.state = CircuitState.CLOSED;
    this.failureCount = 0;
    this.successCount = 0;
    this.errorCount = 0;
    this.requestCount = 0;
    this.lastResetTime = Date.now();
    
    console.log(`[CircuitBreaker ${this.name}] Circuit CLOSED - service recovered`);
    
    logger.logInfo(ErrorCategory.DATABASE, `Circuit breaker ${this.name} closed`, {
      recoveryTime: Date.now() - this.lastFailureTime
    });
  }

  public getState(): CircuitState {
    return this.state;
  }

  public getStats() {
    return {
      state: this.state,
      failureCount: this.failureCount,
      successCount: this.successCount,
      errorRate: this.getErrorRate(),
      requestCount: this.requestCount,
      lastFailureTime: this.lastFailureTime,
      uptime: Date.now() - this.lastResetTime
    };
  }

  public reset(): void {
    this.close();
  }
}

// Create circuit breakers for different Redis operations
export const redisCircuitBreakers = {
  queue: new CircuitBreaker('Redis-Queue', {
    failureThreshold: 5,
    successThreshold: 3,
    timeout: 30000, // 30 seconds
    volumeThreshold: 10,
    errorThresholdPercentage: 50
  }),
  
  cache: new CircuitBreaker('Redis-Cache', {
    failureThreshold: 10,
    successThreshold: 5,
    timeout: 60000, // 1 minute
    volumeThreshold: 20,
    errorThresholdPercentage: 60
  }),
  
  lock: new CircuitBreaker('Redis-Lock', {
    failureThreshold: 3,
    successThreshold: 2,
    timeout: 20000, // 20 seconds
    volumeThreshold: 5,
    errorThresholdPercentage: 40
  })
};

// Helper function to execute Redis operations with circuit breaker
export async function executeWithCircuitBreaker<T>(
  circuitBreaker: CircuitBreaker,
  operation: () => Promise<T>,
  fallback?: () => T | Promise<T>
): Promise<T | null> {
  try {
    return await circuitBreaker.execute(operation);
  } catch (error) {
    // Circuit breaker will handle logging
    console.warn(`[CircuitBreaker] Operation failed with error:`, error);
    
    if (fallback) {
      console.log(`[CircuitBreaker] Attempting fallback for error: ${error instanceof Error ? error.message : String(error)}`);
      try {
        return await fallback();
      } catch (fallbackError) {
        console.error(`[CircuitBreaker] Fallback also failed:`, fallbackError);
        return null;
      }
    }
    return null;
  }
}