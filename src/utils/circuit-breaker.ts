import { debugLog, warnLog, infoLog } from './debug-file-logger';

/**
 * Simple Circuit Breaker implementation to prevent retry storms
 * States: CLOSED (normal), OPEN (failing), HALF_OPEN (testing recovery)
 */
export class CircuitBreaker {
  private state: 'CLOSED' | 'OPEN' | 'HALF_OPEN' = 'CLOSED';
  private failureCount = 0;
  private successCount = 0;
  private nextAttemptTime = 0;
  private readonly failureThreshold: number;
  private readonly resetTimeout: number;
  private readonly halfOpenRequests: number;
  private readonly name: string;
  public lastActivity: number = Date.now();

  constructor(name: string, options?: {
    failureThreshold?: number;
    resetTimeout?: number;
    halfOpenRequests?: number;
  }) {
    this.name = name;
    this.failureThreshold = options?.failureThreshold ?? 10; // Open after 10 failures (increased from 5)
    this.resetTimeout = options?.resetTimeout ?? 30000; // Wait 30s before retry (decreased from 60s)
    this.halfOpenRequests = options?.halfOpenRequests ?? 2; // Test with 2 requests
    debugLog('CircuitBreaker', `CREATED | name=${name} | failureThreshold=${this.failureThreshold} | resetTimeoutMs=${this.resetTimeout} | halfOpenRequests=${this.halfOpenRequests}`);
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    this.lastActivity = Date.now();

    if (this.state === 'OPEN') {
      if (Date.now() < this.nextAttemptTime) {
        const retryIn = Math.round((this.nextAttemptTime - Date.now()) / 1000);
        warnLog('CircuitBreaker', `BLOCKED | name=${this.name} | state=OPEN | retryInSec=${retryIn} | failureCount=${this.failureCount}`);
        throw new Error('Circuit breaker is OPEN - too many failures');
      }
      // Transition to HALF_OPEN to test if service recovered
      infoLog('CircuitBreaker', `STATE_CHANGE | name=${this.name} | OPEN → HALF_OPEN | testing recovery`);
      this.state = 'HALF_OPEN';
      this.successCount = 0;
    }

    try {
      const result = await operation();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  private onSuccess(): void {
    if (this.state === 'HALF_OPEN') {
      this.successCount++;
      debugLog('CircuitBreaker', `HALF_OPEN_SUCCESS | name=${this.name} | successCount=${this.successCount}/${this.halfOpenRequests}`);
      if (this.successCount >= this.halfOpenRequests) {
        // Service recovered, close circuit
        infoLog('CircuitBreaker', `STATE_CHANGE | name=${this.name} | HALF_OPEN → CLOSED | recovered`);
        this.state = 'CLOSED';
        this.successCount = 0;
      }
    }
    this.failureCount = 0;
  }

  private onFailure(): void {
    this.failureCount++;
    debugLog('CircuitBreaker', `FAILURE | name=${this.name} | failureCount=${this.failureCount}/${this.failureThreshold} | state=${this.state}`);

    if (this.state === 'HALF_OPEN') {
      // Failed while testing - reopen circuit
      warnLog('CircuitBreaker', `STATE_CHANGE | name=${this.name} | HALF_OPEN → OPEN | test failed | resetTimeoutMs=${this.resetTimeout}`);
      this.state = 'OPEN';
      this.nextAttemptTime = Date.now() + this.resetTimeout;
      this.failureCount = 0;
      this.successCount = 0;
    } else if (this.failureCount >= this.failureThreshold) {
      // Too many failures - open circuit
      warnLog('CircuitBreaker', `STATE_CHANGE | name=${this.name} | ${this.state} → OPEN | failureCount=${this.failureCount} | resetTimeoutMs=${this.resetTimeout}`);
      this.state = 'OPEN';
      this.nextAttemptTime = Date.now() + this.resetTimeout;
    }
  }

  getState(): 'CLOSED' | 'OPEN' | 'HALF_OPEN' {
    return this.state;
  }

  getName(): string {
    return this.name;
  }

  reset(): void {
    infoLog('CircuitBreaker', `RESET | name=${this.name} | previousState=${this.state}`);
    this.state = 'CLOSED';
    this.failureCount = 0;
    this.successCount = 0;
    this.nextAttemptTime = 0;
  }
}

/**
 * Circuit Breaker Registry for per-collection isolation
 * Instead of a global circuit breaker, each collection gets its own breaker
 * This prevents one failing collection from blocking ALL collections
 */
export class CircuitBreakerRegistry {
  private breakers = new Map<string, CircuitBreaker>();
  private readonly defaultOptions: {
    failureThreshold?: number;
    resetTimeout?: number;
    halfOpenRequests?: number;
  };
  private readonly registryName: string;

  constructor(registryName: string, defaultOptions?: {
    failureThreshold?: number;
    resetTimeout?: number;
    halfOpenRequests?: number;
  }) {
    this.registryName = registryName;
    this.defaultOptions = defaultOptions || {};
    debugLog('CircuitBreaker', `REGISTRY_CREATED | name=${registryName} | defaults=${JSON.stringify(this.defaultOptions)}`);
  }

  /**
   * Get or create a circuit breaker for the given key
   */
  getBreaker(key: string): CircuitBreaker {
    if (!this.breakers.has(key)) {
      const fullName = `${this.registryName}:${key}`;
      this.breakers.set(key, new CircuitBreaker(fullName, this.defaultOptions));
    }
    return this.breakers.get(key)!;
  }

  /**
   * Get stats for all breakers in this registry
   */
  getStats(): { total: number; open: number; halfOpen: number; closed: number } {
    let open = 0, halfOpen = 0, closed = 0;
    for (const breaker of this.breakers.values()) {
      switch (breaker.getState()) {
        case 'OPEN': open++; break;
        case 'HALF_OPEN': halfOpen++; break;
        case 'CLOSED': closed++; break;
      }
    }
    return { total: this.breakers.size, open, halfOpen, closed };
  }

  /**
   * Cleanup stale breakers (closed state, no activity for 5 minutes)
   */
  cleanup(): void {
    const now = Date.now();
    const staleThreshold = 5 * 60 * 1000; // 5 minutes
    let cleanedCount = 0;

    for (const [key, breaker] of this.breakers) {
      if (breaker.getState() === 'CLOSED' && (now - breaker.lastActivity) > staleThreshold) {
        this.breakers.delete(key);
        cleanedCount++;
      }
    }

    if (cleanedCount > 0) {
      debugLog('CircuitBreaker', `REGISTRY_CLEANUP | name=${this.registryName} | cleaned=${cleanedCount} | remaining=${this.breakers.size}`);
    }
  }

  /**
   * Reset a specific breaker
   */
  resetBreaker(key: string): void {
    const breaker = this.breakers.get(key);
    if (breaker) {
      breaker.reset();
    }
  }

  /**
   * Reset all breakers in this registry
   */
  resetAll(): void {
    infoLog('CircuitBreaker', `REGISTRY_RESET_ALL | name=${this.registryName} | count=${this.breakers.size}`);
    for (const breaker of this.breakers.values()) {
      breaker.reset();
    }
  }
}

// Export per-collection circuit breaker registries
export const openseaFloorBreakerRegistry = new CircuitBreakerRegistry('opensea:floor', {
  failureThreshold: 10,   // More tolerant than before (was 5)
  resetTimeout: 30000,    // Faster recovery (was 60s)
  halfOpenRequests: 2     // Need 2 successes to close
});
