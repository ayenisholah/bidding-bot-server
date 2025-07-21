import { logger, ErrorCategory } from './logger';

interface CircuitBreakerOptions {
  failureThreshold: number;
  resetTimeout: number;
  timeout: number;
  name: string;
}

enum CircuitState {
  CLOSED = 'CLOSED',
  OPEN = 'OPEN',
  HALF_OPEN = 'HALF_OPEN'
}

class CircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED;
  private failureCount: number = 0;
  private lastFailureTime: number = 0;
  private successCount: number = 0;
  private nextAttempt: number = Date.now();
  private readonly options: CircuitBreakerOptions;

  constructor(options: CircuitBreakerOptions) {
    this.options = options;
  }

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    if (this.state === CircuitState.OPEN) {
      if (Date.now() < this.nextAttempt) {
        throw new Error(`Circuit breaker is OPEN for ${this.options.name}`);
      }
      // Try to move to half-open
      this.state = CircuitState.HALF_OPEN;
      await logger.logInfo(ErrorCategory.PERFORMANCE, `Circuit breaker ${this.options.name} moved to HALF_OPEN`);
    }

    try {
      // Add timeout wrapper
      const result = await this.executeWithTimeout(fn);
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  private async executeWithTimeout<T>(fn: () => Promise<T>): Promise<T> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`Operation timeout after ${this.options.timeout}ms`));
      }, this.options.timeout);

      fn()
        .then(result => {
          clearTimeout(timer);
          resolve(result);
        })
        .catch(error => {
          clearTimeout(timer);
          reject(error);
        });
    });
  }

  private onSuccess() {
    this.failureCount = 0;
    
    if (this.state === CircuitState.HALF_OPEN) {
      this.successCount++;
      if (this.successCount >= 3) {
        // Require 3 successful calls to fully close
        this.state = CircuitState.CLOSED;
        this.successCount = 0;
        logger.logInfo(ErrorCategory.PERFORMANCE, `Circuit breaker ${this.options.name} moved to CLOSED`).catch(() => {});
      }
    }
  }

  private onFailure() {
    this.failureCount++;
    this.lastFailureTime = Date.now();
    
    if (this.state === CircuitState.HALF_OPEN) {
      // Immediate open on failure in half-open state
      this.state = CircuitState.OPEN;
      this.nextAttempt = Date.now() + this.options.resetTimeout;
      this.successCount = 0;
      logger.logWarning(ErrorCategory.PERFORMANCE, `Circuit breaker ${this.options.name} moved to OPEN (half-open failure)`).catch(() => {});
    } else if (this.failureCount >= this.options.failureThreshold) {
      this.state = CircuitState.OPEN;
      this.nextAttempt = Date.now() + this.options.resetTimeout;
      logger.logWarning(ErrorCategory.PERFORMANCE, `Circuit breaker ${this.options.name} moved to OPEN (threshold reached)`).catch(() => {});
    }
  }

  getState(): CircuitState {
    return this.state;
  }

  getStats() {
    return {
      state: this.state,
      failureCount: this.failureCount,
      successCount: this.successCount,
      lastFailureTime: this.lastFailureTime,
      nextAttempt: this.nextAttempt
    };
  }

  reset() {
    this.state = CircuitState.CLOSED;
    this.failureCount = 0;
    this.successCount = 0;
    this.lastFailureTime = 0;
    this.nextAttempt = Date.now();
  }
}

// Factory for creating circuit breakers
class CircuitBreakerFactory {
  private static breakers: Map<string, CircuitBreaker> = new Map();

  static create(name: string, options?: Partial<CircuitBreakerOptions>): CircuitBreaker {
    if (!this.breakers.has(name)) {
      const defaultOptions: CircuitBreakerOptions = {
        failureThreshold: 5,
        resetTimeout: 30000, // 30 seconds
        timeout: 10000, // 10 seconds
        name,
        ...options
      };
      
      this.breakers.set(name, new CircuitBreaker(defaultOptions));
    }
    
    return this.breakers.get(name)!;
  }

  static get(name: string): CircuitBreaker | undefined {
    return this.breakers.get(name);
  }

  static getAll(): Map<string, CircuitBreaker> {
    return this.breakers;
  }

  static reset(name: string) {
    const breaker = this.breakers.get(name);
    if (breaker) {
      breaker.reset();
    }
  }

  static resetAll() {
    for (const breaker of this.breakers.values()) {
      breaker.reset();
    }
  }
}

export { CircuitBreaker, CircuitBreakerFactory, CircuitState, CircuitBreakerOptions };