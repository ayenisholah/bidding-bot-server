import { EventEmitter } from 'events';

interface WalletRateLimit {
  remaining: number;
  resetTime: number;
  limit: number;
  lastRequest: number;
  isRateLimited: boolean;
}

interface WalletErrorTracking {
  errors: number;
  successes: number;
  lastError: number;
  lastSuccess: number;
}

interface RateLimitEvent {
  walletAddress: string;
  remaining: number;
  resetTime: number;
  limit: number;
}

export class BlurRateLimiter extends EventEmitter {
  private walletLimits: Map<string, WalletRateLimit>;
  private walletErrors: Map<string, WalletErrorTracking>;
  private pendingRequests: Map<string, number>; // Track in-flight requests per wallet
  private readonly DEFAULT_LIMIT = 40; // Blur's actual limit
  private readonly DEFAULT_WINDOW = 10000; // 10 seconds
  private readonly SAFETY_THRESHOLD = 0.8; // Use 80% of rate limit for safety buffer
  

  constructor() {
    super();
    this.walletLimits = new Map();
    this.walletErrors = new Map();
    this.pendingRequests = new Map();
  }

  private createWalletLimit(): WalletRateLimit {
    const effectiveLimit = Math.floor(this.DEFAULT_LIMIT * this.SAFETY_THRESHOLD);
    return {
      remaining: effectiveLimit,
      resetTime: Date.now() + this.DEFAULT_WINDOW,
      limit: this.DEFAULT_LIMIT,
      lastRequest: Date.now(),
      isRateLimited: false
    };
  }

  private cleanupOldEntries(): void {
    const now = Date.now();
    for (const [wallet, limit] of this.walletLimits.entries()) {
      // Remove entries that haven't been used for over an hour
      if (now - limit.lastRequest > 3600000) {
        this.walletLimits.delete(wallet);
        this.walletErrors.delete(wallet);
      }
    }
  }

  canMakeRequest(walletAddress: string): boolean {
    this.cleanupOldEntries();
    
    const wallet = this.walletLimits.get(walletAddress);
    if (!wallet) {
      return true;
    }

    const now = Date.now();
    
    // If reset time has passed, reset the limit
    if (now >= wallet.resetTime) {
      wallet.remaining = Math.floor(wallet.limit * this.SAFETY_THRESHOLD);
      wallet.resetTime = now + this.DEFAULT_WINDOW;
      wallet.isRateLimited = false;
    }

    // Get pending requests count
    const pendingCount = this.pendingRequests.get(walletAddress) || 0;
    
    // Calculate effective remaining slots (actual remaining minus pending)
    const effectiveRemaining = wallet.remaining - pendingCount;
    
    // Check against safety threshold to avoid hitting hard limits
    const safetyBuffer = Math.floor(wallet.limit * (1 - this.SAFETY_THRESHOLD));
    return effectiveRemaining > safetyBuffer && !wallet.isRateLimited;
  }


  recordRequest(walletAddress: string): void {
    let wallet = this.walletLimits.get(walletAddress);
    
    if (!wallet) {
      wallet = this.createWalletLimit();
      this.walletLimits.set(walletAddress, wallet);
    }

    wallet.remaining = Math.max(0, wallet.remaining - 1);
    wallet.lastRequest = Date.now();

    this.emit('request', {
      walletAddress,
      remaining: wallet.remaining,
      resetTime: wallet.resetTime,
      limit: wallet.limit
    } as RateLimitEvent);
  }

  updateFromHeaders(walletAddress: string, headers: {
    remaining: number;
    reset: number;
    limit: number;
  }): void {
    let wallet = this.walletLimits.get(walletAddress);
    
    if (!wallet) {
      wallet = this.createWalletLimit();
      this.walletLimits.set(walletAddress, wallet);
    }

    // Update with actual values from API
    wallet.remaining = headers.remaining;
    wallet.resetTime = Date.now() + (headers.reset * 1000);
    wallet.limit = headers.limit;
    wallet.lastRequest = Date.now();
    
    // Check if we're rate limited based on remaining
    wallet.isRateLimited = headers.remaining === 0;

    this.emit('update', {
      walletAddress,
      remaining: wallet.remaining,
      resetTime: wallet.resetTime,
      limit: wallet.limit
    } as RateLimitEvent);
  }

  markRateLimited(walletAddress: string, retryAfter?: number): void {
    let wallet = this.walletLimits.get(walletAddress);
    
    if (!wallet) {
      wallet = this.createWalletLimit();
      this.walletLimits.set(walletAddress, wallet);
    }

    wallet.isRateLimited = true;
    wallet.remaining = 0;
    
    if (retryAfter) {
      wallet.resetTime = Date.now() + (retryAfter * 1000);
    }

    this.emit('rate-limited', {
      walletAddress,
      remaining: 0,
      resetTime: wallet.resetTime,
      limit: wallet.limit
    } as RateLimitEvent);
  }

  getWaitTime(walletAddress: string): number {
    const wallet = this.walletLimits.get(walletAddress);
    if (!wallet || !wallet.isRateLimited) {
      return 0;
    }

    const now = Date.now();
    if (now >= wallet.resetTime) {
      return 0;
    }

    // Add jitter (10-20% randomization) to prevent thundering herd
    const baseWaitTime = wallet.resetTime - now;
    const jitterFactor = 0.1 + (Math.random() * 0.1); // 10-20% jitter
    const jitter = Math.floor(baseWaitTime * jitterFactor);
    
    return baseWaitTime + jitter;
  }

  /**
   * Atomically reserve a request slot for a wallet
   * Returns true if reservation successful, false otherwise
   */
  reserveRequest(walletAddress: string): boolean {
    this.cleanupOldEntries();
    
    // First check if we can make request
    if (!this.canMakeRequest(walletAddress)) {
      return false;
    }
    
    // Get or create wallet limit
    let wallet = this.walletLimits.get(walletAddress);
    if (!wallet) {
      wallet = this.createWalletLimit();
      this.walletLimits.set(walletAddress, wallet);
    }
    
    // Check again with current state
    const pendingCount = this.pendingRequests.get(walletAddress) || 0;
    const effectiveRemaining = wallet.remaining - pendingCount;
    const safetyBuffer = Math.floor(wallet.limit * (1 - this.SAFETY_THRESHOLD));
    
    if (effectiveRemaining <= safetyBuffer || wallet.isRateLimited) {
      return false;
    }
    
    // Reserve the slot
    this.pendingRequests.set(walletAddress, pendingCount + 1);
    
    this.emit('request-reserved', {
      walletAddress,
      remaining: wallet.remaining,
      pending: pendingCount + 1,
      effectiveRemaining: effectiveRemaining - 1
    });
    
    return true;
  }

  /**
   * Release a reserved request slot (call this in finally block)
   */
  releaseRequest(walletAddress: string, wasSuccessful: boolean = true): void {
    const pendingCount = this.pendingRequests.get(walletAddress) || 0;
    if (pendingCount > 0) {
      this.pendingRequests.set(walletAddress, pendingCount - 1);
    }
    
    // If request was successful, record it normally
    if (wasSuccessful) {
      this.recordRequest(walletAddress);
    }
    
    // Clean up if no pending requests
    if (pendingCount <= 1) {
      this.pendingRequests.delete(walletAddress);
    }
    
    this.emit('request-released', {
      walletAddress,
      wasSuccessful,
      pendingRemaining: Math.max(0, pendingCount - 1)
    });
  }

  getStatus(walletAddress?: string): any {
    if (walletAddress) {
      const wallet = this.walletLimits.get(walletAddress);
      const pendingCount = this.pendingRequests.get(walletAddress) || 0;
      
      if (!wallet) {
        return {
          walletAddress,
          status: 'not_tracked',
          canMakeRequest: true,
          pendingRequests: pendingCount
        };
      }

      return {
        walletAddress,
        remaining: wallet.remaining,
        resetTime: new Date(wallet.resetTime).toISOString(),
        limit: wallet.limit,
        isRateLimited: wallet.isRateLimited,
        canMakeRequest: this.canMakeRequest(walletAddress),
        waitTime: this.getWaitTime(walletAddress),
        pendingRequests: pendingCount,
        effectiveRemaining: wallet.remaining - pendingCount
      };
    }

    // Return status for all wallets
    const statuses: any[] = [];
    for (const [address, wallet] of this.walletLimits.entries()) {
      const pendingCount = this.pendingRequests.get(address) || 0;
      statuses.push({
        walletAddress: address,
        remaining: wallet.remaining,
        resetTime: new Date(wallet.resetTime).toISOString(),
        limit: wallet.limit,
        isRateLimited: wallet.isRateLimited,
        canMakeRequest: this.canMakeRequest(address),
        waitTime: this.getWaitTime(address),
        pendingRequests: pendingCount,
        effectiveRemaining: wallet.remaining - pendingCount
      });
    }

    return {
      totalWallets: this.walletLimits.size,
      rateLimitedWallets: statuses.filter(s => s.isRateLimited).length,
      wallets: statuses
    };
  }

  reset(walletAddress: string): void {
    const wallet = this.walletLimits.get(walletAddress);
    if (wallet) {
      wallet.remaining = Math.floor(wallet.limit * this.SAFETY_THRESHOLD);
      wallet.resetTime = Date.now() + this.DEFAULT_WINDOW;
      wallet.isRateLimited = false;
      
      // Also clear pending requests on reset
      this.pendingRequests.delete(walletAddress);
      
      this.emit('reset', {
        walletAddress,
        remaining: wallet.remaining,
        resetTime: wallet.resetTime,
        limit: wallet.limit
      } as RateLimitEvent);
    }
  }

  resetAll(): void {
    for (const [address] of this.walletLimits.entries()) {
      this.reset(address);
    }
  }

  // Record error or success for wallet tracking
  recordError(walletAddress: string, isError: boolean): void {
    let errorTracking = this.walletErrors.get(walletAddress);
    
    if (!errorTracking) {
      errorTracking = {
        errors: 0,
        successes: 0,
        lastError: 0,
        lastSuccess: 0
      };
      this.walletErrors.set(walletAddress, errorTracking);
    }

    const now = Date.now();
    
    if (isError) {
      errorTracking.errors++;
      errorTracking.lastError = now;
      
      // Emit error event for monitoring
      this.emit('error-recorded', {
        walletAddress,
        totalErrors: errorTracking.errors,
        totalSuccesses: errorTracking.successes,
        errorRate: this.getErrorRate(walletAddress)
      });
    } else {
      errorTracking.successes++;
      errorTracking.lastSuccess = now;
    }
  }

  // Get error rate for a wallet
  getErrorRate(walletAddress: string): number {
    const errorTracking = this.walletErrors.get(walletAddress);
    if (!errorTracking) return 0;
    
    const total = errorTracking.errors + errorTracking.successes;
    if (total === 0) return 0;
    
    return (errorTracking.errors / total) * 100;
  }

  // Get error statistics for monitoring
  getErrorStats(walletAddress?: string): any {
    if (walletAddress) {
      const errorTracking = this.walletErrors.get(walletAddress);
      if (!errorTracking) {
        return {
          walletAddress,
          errors: 0,
          successes: 0,
          errorRate: 0,
          lastError: null,
          lastSuccess: null
        };
      }

      return {
        walletAddress,
        errors: errorTracking.errors,
        successes: errorTracking.successes,
        errorRate: this.getErrorRate(walletAddress),
        lastError: errorTracking.lastError ? new Date(errorTracking.lastError).toISOString() : null,
        lastSuccess: errorTracking.lastSuccess ? new Date(errorTracking.lastSuccess).toISOString() : null
      };
    }

    // Return stats for all wallets
    const stats: any[] = [];
    for (const [address, errorTracking] of this.walletErrors.entries()) {
      stats.push({
        walletAddress: address,
        errors: errorTracking.errors,
        successes: errorTracking.successes,
        errorRate: this.getErrorRate(address),
        lastError: errorTracking.lastError ? new Date(errorTracking.lastError).toISOString() : null,
        lastSuccess: errorTracking.lastSuccess ? new Date(errorTracking.lastSuccess).toISOString() : null
      });
    }

    return {
      totalWallets: this.walletErrors.size,
      wallets: stats.sort((a, b) => b.errorRate - a.errorRate) // Sort by error rate descending
    };
  }
}

// Singleton instance
export const blurRateLimiter = new BlurRateLimiter();