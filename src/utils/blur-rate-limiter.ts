import { EventEmitter } from 'events';

interface WalletRateLimit {
  remaining: number;
  resetTime: number;
  limit: number;
  lastRequest: number;
  isRateLimited: boolean;
}

interface RateLimitEvent {
  walletAddress: string;
  remaining: number;
  resetTime: number;
  limit: number;
}

export class BlurRateLimiter extends EventEmitter {
  private walletLimits: Map<string, WalletRateLimit>;
  private readonly DEFAULT_LIMIT = 40;
  private readonly DEFAULT_WINDOW = 10000; // 10 seconds
  private readonly SAFETY_THRESHOLD = 0.7; // Start limiting at 70% usage
  private requestCounts: Map<string, { count: number; windowStart: number }>;

  constructor() {
    super();
    this.walletLimits = new Map();
    this.requestCounts = new Map();
  }

  private createWalletLimit(): WalletRateLimit {
    return {
      remaining: this.DEFAULT_LIMIT,
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
      }
    }
  }

  canMakeRequest(walletAddress: string): boolean {
    this.cleanupOldEntries();
    
    // Check local request count first
    const localCount = this.getLocalRequestCount(walletAddress);
    if (localCount >= this.DEFAULT_LIMIT * this.SAFETY_THRESHOLD) {
      console.log(`[BlurRateLimiter] Local rate limit: ${localCount}/${this.DEFAULT_LIMIT} requests in window`);
      return false;
    }
    
    const wallet = this.walletLimits.get(walletAddress);
    if (!wallet) {
      return true;
    }

    const now = Date.now();
    
    // If reset time has passed, reset the limit
    if (now >= wallet.resetTime) {
      wallet.remaining = wallet.limit;
      wallet.resetTime = now + this.DEFAULT_WINDOW;
      wallet.isRateLimited = false;
    }

    // Check if we're approaching the limit (safety threshold)
    const usageRatio = 1 - (wallet.remaining / wallet.limit);
    if (usageRatio >= this.SAFETY_THRESHOLD) {
      console.log(`[BlurRateLimiter] Approaching limit: ${wallet.remaining}/${wallet.limit} remaining`);
      return false;
    }

    return wallet.remaining > 0 && !wallet.isRateLimited;
  }

  private getLocalRequestCount(walletAddress: string): number {
    const counter = this.requestCounts.get(walletAddress);
    if (!counter) return 0;
    
    const now = Date.now();
    // If window has passed, reset counter
    if (now - counter.windowStart >= this.DEFAULT_WINDOW) {
      this.requestCounts.set(walletAddress, { count: 0, windowStart: now });
      return 0;
    }
    
    return counter.count;
  }

  recordRequest(walletAddress: string): void {
    // Update local counter
    const now = Date.now();
    let counter = this.requestCounts.get(walletAddress);
    if (!counter || now - counter.windowStart >= this.DEFAULT_WINDOW) {
      counter = { count: 0, windowStart: now };
    }
    counter.count++;
    this.requestCounts.set(walletAddress, counter);
    
    // Update wallet limits
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

    return wallet.resetTime - now;
  }

  getStatus(walletAddress?: string): any {
    if (walletAddress) {
      const wallet = this.walletLimits.get(walletAddress);
      if (!wallet) {
        return {
          walletAddress,
          status: 'not_tracked',
          canMakeRequest: true
        };
      }

      return {
        walletAddress,
        remaining: wallet.remaining,
        resetTime: new Date(wallet.resetTime).toISOString(),
        limit: wallet.limit,
        isRateLimited: wallet.isRateLimited,
        canMakeRequest: this.canMakeRequest(walletAddress),
        waitTime: this.getWaitTime(walletAddress)
      };
    }

    // Return status for all wallets
    const statuses: any[] = [];
    for (const [address, wallet] of this.walletLimits.entries()) {
      statuses.push({
        walletAddress: address,
        remaining: wallet.remaining,
        resetTime: new Date(wallet.resetTime).toISOString(),
        limit: wallet.limit,
        isRateLimited: wallet.isRateLimited,
        canMakeRequest: this.canMakeRequest(address),
        waitTime: this.getWaitTime(address)
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
      wallet.remaining = wallet.limit;
      wallet.resetTime = Date.now() + this.DEFAULT_WINDOW;
      wallet.isRateLimited = false;
      
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
}

// Singleton instance
export const blurRateLimiter = new BlurRateLimiter();