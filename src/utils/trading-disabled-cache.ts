import { BoundedCache } from './bounded-cache';

export interface TradingDisabledInfo {
  contractAddress: string;
  tokenId: number;
  marketplace: string;
  disabledAt: number;
  errorMessage: string;
}

/**
 * Cache for tracking NFT tokens that have "Trading is disabled" errors.
 * Prevents repeated bid attempts on tokens that OpenSea has restricted.
 * Cache entries expire after 24 hours to allow retry in case restrictions are lifted.
 * (Increased from 1 hour as trading restrictions rarely change quickly)
 */
class TradingDisabledCache {
  private cache: BoundedCache<string, TradingDisabledInfo>;
  private readonly CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours (increased from 1 hour for better efficiency)

  constructor() {
    // Max 5000 tokens with 24 hour TTL
    this.cache = new BoundedCache<string, TradingDisabledInfo>(5000, this.CACHE_TTL);
  }

  /**
   * Generate cache key from contract, token, and marketplace
   */
  private getKey(contractAddress: string, tokenId: number, marketplace: string): string {
    return `${contractAddress.toLowerCase()}-${tokenId}-${marketplace.toLowerCase()}`;
  }

  /**
   * Check if a token is currently disabled for trading
   */
  isDisabled(contractAddress: string, tokenId: number, marketplace: string): boolean {
    const key = this.getKey(contractAddress, tokenId, marketplace);
    return this.cache.has(key);
  }

  /**
   * Mark a token as disabled for trading
   */
  markAsDisabled(contractAddress: string, tokenId: number, marketplace: string, errorMessage: string): void {
    const key = this.getKey(contractAddress, tokenId, marketplace);
    this.cache.set(key, {
      contractAddress: contractAddress.toLowerCase(),
      tokenId,
      marketplace: marketplace.toLowerCase(),
      disabledAt: Date.now(),
      errorMessage
    });
  }

  /**
   * Get all currently disabled tokens
   */
  getDisabledTokens(): TradingDisabledInfo[] {
    const results: TradingDisabledInfo[] = [];
    for (const key of this.cache.keys()) {
      const value = this.cache.get(key);
      if (value) {
        results.push(value);
      }
    }
    return results;
  }

  /**
   * Get statistics about disabled tokens
   */
  getStats(): { total: number; byMarketplace: Record<string, number> } {
    const tokens = this.getDisabledTokens();
    const byMarketplace: Record<string, number> = {};

    tokens.forEach(token => {
      byMarketplace[token.marketplace] = (byMarketplace[token.marketplace] || 0) + 1;
    });

    return {
      total: tokens.length,
      byMarketplace
    };
  }

  /**
   * Clear all cached disabled tokens
   */
  clear(): void {
    this.cache.clear();
  }

  /**
   * Get detailed cache statistics
   */
  getCacheStats() {
    return this.cache.getStats();
  }
}

// Export singleton instance
export const tradingDisabledCache = new TradingDisabledCache();