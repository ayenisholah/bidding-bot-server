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

  constructor(options?: {
    failureThreshold?: number;
    resetTimeout?: number;
    halfOpenRequests?: number;
  }) {
    this.failureThreshold = options?.failureThreshold ?? 5; // Open after 5 failures
    this.resetTimeout = options?.resetTimeout ?? 60000; // Wait 60s before retry
    this.halfOpenRequests = options?.halfOpenRequests ?? 3; // Test with 3 requests
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    if (this.state === 'OPEN') {
      if (Date.now() < this.nextAttemptTime) {
        throw new Error('Circuit breaker is OPEN - too many failures');
      }
      // Transition to HALF_OPEN to test if service recovered
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
    this.failureCount = 0;

    if (this.state === 'HALF_OPEN') {
      this.successCount++;
      if (this.successCount >= this.halfOpenRequests) {
        // Service recovered, close circuit
        this.state = 'CLOSED';
        this.successCount = 0;
      }
    }
  }

  private onFailure(): void {
    this.failureCount++;

    if (this.state === 'HALF_OPEN') {
      // Failed while testing - reopen circuit
      this.state = 'OPEN';
      this.nextAttemptTime = Date.now() + this.resetTimeout;
      this.failureCount = 0;
      this.successCount = 0;
    } else if (this.failureCount >= this.failureThreshold) {
      // Too many failures - open circuit
      this.state = 'OPEN';
      this.nextAttemptTime = Date.now() + this.resetTimeout;
    }
  }

  getState(): 'CLOSED' | 'OPEN' | 'HALF_OPEN' {
    return this.state;
  }

  reset(): void {
    this.state = 'CLOSED';
    this.failureCount = 0;
    this.successCount = 0;
    this.nextAttemptTime = 0;
  }
}
