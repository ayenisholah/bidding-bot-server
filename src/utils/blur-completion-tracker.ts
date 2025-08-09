/**
 * Tracks Blur job completions per wallet to implement rate limiting
 * - 36 bids maximum per 10-second window
 * - Window resets every 10 seconds
 * - If limit reached, delay until next window
 */
export class BlurCompletionTracker {
  private completions: Map<string, { 
    windowStartTime: number,
    windowBidCount: number 
  }> = new Map();

  // Rate limiting thresholds
  private readonly MAX_BIDS_PER_WINDOW = 36; // Maximum bids allowed per window
  private readonly WINDOW_SIZE_MS = 10000; // 10-second window
  private readonly CLEANUP_INTERVAL_MS = 60000; // 1 minute

  private cleanupTimer: NodeJS.Timeout | null = null;

  constructor() {
    // Start periodic cleanup
    this.startCleanup();
  }

  /**
   * Track a job completion for a wallet
   */
  trackCompletion(walletAddress: string): void {
    const wallet = walletAddress.toLowerCase();
    const now = Date.now();
    
    let data = this.completions.get(wallet);
    
    // Initialize or reset window if needed
    if (!data || (now - data.windowStartTime >= this.WINDOW_SIZE_MS)) {
      data = { 
        windowStartTime: Math.floor(now / this.WINDOW_SIZE_MS) * this.WINDOW_SIZE_MS,
        windowBidCount: 0 
      };
      this.completions.set(wallet, data);
    }
    
    // Increment bid count for current window
    data.windowBidCount++;
    
    console.log(`[Blur Completion] Wallet ${wallet} - Window: ${data.windowBidCount}/${this.MAX_BIDS_PER_WINDOW}, Window ends in ${this.WINDOW_SIZE_MS - (now - data.windowStartTime)}ms`);
  }

  /**
   * Get required delay for a wallet based on completion rates
   */
  getRequiredDelay(walletAddress: string): number {
    const wallet = walletAddress.toLowerCase();
    const now = Date.now();
    const data = this.completions.get(wallet);
    
    if (!data) {
      return 0; // No completions tracked, no delay needed
    }
    
    // Check if current window has expired
    if (now - data.windowStartTime >= this.WINDOW_SIZE_MS) {
      return 0; // Window has expired, allow immediate bid
    }
    
    // Check if we've hit the limit for current window
    if (data.windowBidCount >= this.MAX_BIDS_PER_WINDOW) {
      // Calculate remaining time until window resets
      const remainingMs = this.WINDOW_SIZE_MS - (now - data.windowStartTime);
      console.log(`[Blur Completion] Wallet ${wallet} hit limit (${data.windowBidCount}/${this.MAX_BIDS_PER_WINDOW}) - waiting ${remainingMs}ms for window reset`);
      return remainingMs;
    }
    
    return 0; // Under limit, no delay needed
  }

  /**
   * Get completion statistics for a wallet
   */
  getStats(walletAddress?: string): any {
    const now = Date.now();
    
    if (walletAddress) {
      const wallet = walletAddress.toLowerCase();
      const data = this.completions.get(wallet);
      
      if (!data) {
        return {
          wallet,
          windowBidCount: 0,
          windowRemaining: this.MAX_BIDS_PER_WINDOW,
          windowTimeRemaining: 0,
          requiredDelay: 0
        };
      }
      
      const windowExpired = now - data.windowStartTime >= this.WINDOW_SIZE_MS;
      const windowTimeRemaining = windowExpired ? 0 : this.WINDOW_SIZE_MS - (now - data.windowStartTime);
      
      return {
        wallet,
        windowBidCount: windowExpired ? 0 : data.windowBidCount,
        windowRemaining: windowExpired ? this.MAX_BIDS_PER_WINDOW : this.MAX_BIDS_PER_WINDOW - data.windowBidCount,
        windowTimeRemaining,
        requiredDelay: this.getRequiredDelay(wallet)
      };
    }
    
    // Return stats for all wallets
    const stats: any[] = [];
    for (const [wallet, data] of this.completions.entries()) {
      const windowExpired = now - data.windowStartTime >= this.WINDOW_SIZE_MS;
      const windowTimeRemaining = windowExpired ? 0 : this.WINDOW_SIZE_MS - (now - data.windowStartTime);
      
      stats.push({
        wallet,
        windowBidCount: windowExpired ? 0 : data.windowBidCount,
        windowRemaining: windowExpired ? this.MAX_BIDS_PER_WINDOW : this.MAX_BIDS_PER_WINDOW - data.windowBidCount,
        windowTimeRemaining,
        requiredDelay: this.getRequiredDelay(wallet)
      });
    }
    
    return {
      totalWallets: this.completions.size,
      wallets: stats.sort((a, b) => b.windowBidCount - a.windowBidCount)
    };
  }

  /**
   * Reset completion tracking for a wallet
   */
  reset(walletAddress?: string): void {
    if (walletAddress) {
      this.completions.delete(walletAddress.toLowerCase());
      console.log(`[Blur Completion] Reset tracking for wallet ${walletAddress}`);
    } else {
      this.completions.clear();
      console.log('[Blur Completion] Reset all completion tracking');
    }
  }


  /**
   * Start periodic cleanup of old data
   */
  private startCleanup(): void {
    this.cleanupTimer = setInterval(() => {
      this.cleanup();
    }, this.CLEANUP_INTERVAL_MS);
  }

  /**
   * Clean up old data from all wallets
   */
  cleanup(): void {
    const now = Date.now();
    let cleaned = 0;
    
    for (const [wallet, data] of this.completions.entries()) {
      // Remove wallet data if window is more than 1 hour old and has 0 bids
      if (now - data.windowStartTime > 3600000 && data.windowBidCount === 0) {
        this.completions.delete(wallet);
        cleaned++;
      }
    }
    
    if (cleaned > 0) {
      console.log(`[Blur Completion] Cleaned up ${cleaned} inactive wallets`);
    }
  }

  /**
   * Stop the cleanup timer
   */
  destroy(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
  }
}

// Singleton instance
export const blurCompletionTracker = new BlurCompletionTracker();