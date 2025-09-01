import { EventEmitter } from 'events';

interface CircuitBreakerOptions {
  failureThreshold: number;
  successThreshold: number;
  timeout: number;
  monitoringPeriod: number;
}

enum CircuitState {
  CLOSED = 'CLOSED',
  OPEN = 'OPEN',
  HALF_OPEN = 'HALF_OPEN'
}

export class CircuitBreaker extends EventEmitter {
  private state: CircuitState = CircuitState.CLOSED;
  private failureCount: number = 0;
  private successCount: number = 0;
  private lastFailureTime: number = 0;
  private openTime: number = 0;
  private requests: Map<string, { failures: number; successes: number; timestamp: number }>;
  
  private readonly options: CircuitBreakerOptions;
  
  constructor(options: Partial<CircuitBreakerOptions> = {}) {
    super();
    this.options = {
      failureThreshold: 5,
      successThreshold: 3,
      timeout: 60000, // 60 seconds
      monitoringPeriod: 300000, // 5 minutes
      ...options
    };
    this.requests = new Map();
  }
  
  async execute<T>(fn: () => Promise<T>, context: string = 'default'): Promise<T> {
    if (!this.canExecute()) {
      this.emit('rejected', { context, state: this.state });
      throw new Error(`Circuit breaker is ${this.state}. Request rejected.`);
    }
    
    try {
      const result = await fn();
      this.onSuccess(context);
      return result;
    } catch (error) {
      this.onFailure(context);
      throw error;
    }
  }
  
  private canExecute(): boolean {
    if (this.state === CircuitState.CLOSED) {
      return true;
    }
    
    if (this.state === CircuitState.OPEN) {
      const now = Date.now();
      if (now - this.openTime >= this.options.timeout) {
        this.transition(CircuitState.HALF_OPEN);
        return true;
      }
      return false;
    }
    
    // HALF_OPEN state
    return true;
  }
  
  private onSuccess(context: string): void {
    this.recordRequest(context, false);
    
    if (this.state === CircuitState.HALF_OPEN) {
      this.successCount++;
      if (this.successCount >= this.options.successThreshold) {
        this.transition(CircuitState.CLOSED);
      }
    } else if (this.state === CircuitState.CLOSED) {
      this.failureCount = Math.max(0, this.failureCount - 1);
    }
  }
  
  private onFailure(context: string): void {
    this.recordRequest(context, true);
    this.lastFailureTime = Date.now();
    
    if (this.state === CircuitState.HALF_OPEN) {
      this.transition(CircuitState.OPEN);
    } else if (this.state === CircuitState.CLOSED) {
      this.failureCount++;
      if (this.failureCount >= this.options.failureThreshold) {
        this.transition(CircuitState.OPEN);
      }
    }
  }
  
  private transition(newState: CircuitState): void {
    const oldState = this.state;
    this.state = newState;
    
    switch (newState) {
      case CircuitState.OPEN:
        this.openTime = Date.now();
        this.emit('open', { oldState, newState });
        break;
      case CircuitState.HALF_OPEN:
        this.successCount = 0;
        this.failureCount = 0;
        this.emit('halfOpen', { oldState, newState });
        break;
      case CircuitState.CLOSED:
        this.successCount = 0;
        this.failureCount = 0;
        this.emit('closed', { oldState, newState });
        break;
    }
  }
  
  private recordRequest(context: string, isFailure: boolean): void {
    const now = Date.now();
    let record = this.requests.get(context);
    
    if (!record || now - record.timestamp > this.options.monitoringPeriod) {
      record = { failures: 0, successes: 0, timestamp: now };
    }
    
    if (isFailure) {
      record.failures++;
    } else {
      record.successes++;
    }
    
    this.requests.set(context, record);
    
    // Clean up old entries
    for (const [key, value] of this.requests.entries()) {
      if (now - value.timestamp > this.options.monitoringPeriod) {
        this.requests.delete(key);
      }
    }
  }
  
  getState(): CircuitState {
    return this.state;
  }
  
  getStats(context?: string): any {
    if (context) {
      const record = this.requests.get(context);
      if (!record) {
        return { context, failures: 0, successes: 0, rate: 0 };
      }
      
      const total = record.failures + record.successes;
      return {
        context,
        failures: record.failures,
        successes: record.successes,
        rate: total > 0 ? (record.failures / total) * 100 : 0
      };
    }
    
    // Return overall stats
    let totalFailures = 0;
    let totalSuccesses = 0;
    
    for (const record of this.requests.values()) {
      totalFailures += record.failures;
      totalSuccesses += record.successes;
    }
    
    const total = totalFailures + totalSuccesses;
    
    return {
      state: this.state,
      failureCount: this.failureCount,
      successCount: this.successCount,
      totalRequests: total,
      totalFailures,
      totalSuccesses,
      failureRate: total > 0 ? (totalFailures / total) * 100 : 0,
      contexts: Array.from(this.requests.keys())
    };
  }
  
  reset(): void {
    this.state = CircuitState.CLOSED;
    this.failureCount = 0;
    this.successCount = 0;
    this.lastFailureTime = 0;
    this.openTime = 0;
    this.requests.clear();
    this.emit('reset');
  }
}

// Create instances for different services
export const blurCircuitBreaker = new CircuitBreaker({
  failureThreshold: 10, // Open after 10 failures
  successThreshold: 5,  // Close after 5 successes in half-open
  timeout: 30000,       // Try half-open after 30 seconds
  monitoringPeriod: 300000 // 5 minutes
});

// Log circuit breaker events
blurCircuitBreaker.on('open', (data) => {
  // console.log('[CircuitBreaker] Blur circuit opened:', data);
});

blurCircuitBreaker.on('halfOpen', (data) => {
  // console.log('[CircuitBreaker] Blur circuit half-open:', data);
});

blurCircuitBreaker.on('closed', (data) => {
  // console.log('[CircuitBreaker] Blur circuit closed:', data);
});