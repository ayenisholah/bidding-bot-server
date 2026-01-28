import { LRUCache } from 'lru-cache';
import { getCacheStats } from './lru-helpers';

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
 * Cache entries expire after 4 hours to allow retry in case restrictions are lifted.
 * (Reduced from 24 hours as OpenSea restrictions typically lift within 2-12 hours)
 */
class TradingDisabledCache {
  private cache: LRUCache<string, TradingDisabledInfo>;
  private readonly CACHE_TTL = 4 * 60 * 60 * 1000; // 4 hours (optimal for retry window)

  constructor() {
    // Max 5000 tokens with 4 hour TTL
    this.cache = new LRUCache<string, TradingDisabledInfo>({
      max: 5000,
      ttl: this.CACHE_TTL
    });
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
    for (const key of Array.from(this.cache.keys())) {
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
    return getCacheStats(this.cache);
  }
}

// Export singleton instance
export const tradingDisabledCache = new TradingDisabledCache();