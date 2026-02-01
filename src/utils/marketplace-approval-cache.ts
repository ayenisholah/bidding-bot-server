/**
 * Marketplace Approval Status Cache
 *
 * Tracks boolean approval status flags for wallet-currency combinations across
 * OpenSea and MagicEden. Acts as a "done" flag after approval transactions complete.
 *
 * NOT to be confused with approvalCache which tracks actual ERC20 allowance
 * amounts (numeric) via on-chain RPC calls.
 */
import { LRUCache } from 'lru-cache';
import { debugLog } from './debug-file-logger';

export interface MarketplaceApprovalEntry {
  address: string;
  currency: string;
  openseaApproval: boolean;
  magicedenApproval: boolean;
  timestamp: Date;
}

interface MarketplaceApprovalCacheStats {
  hits: number;
  misses: number;
  sets: number;
  totalRequests: number;
  hitRate: string;
  size: number;
  maxSize: number;
}

class MarketplaceApprovalCache {
  // Cache: key = address-currency
  private marketplaceApprovalLRU: LRUCache<string, MarketplaceApprovalEntry>;

  // Stats tracking
  private stats = {
    hits: 0,
    misses: 0,
    sets: 0,
    totalRequests: 0
  };

  constructor() {
    this.marketplaceApprovalLRU = new LRUCache<string, MarketplaceApprovalEntry>({
      max: 1000, // Max 1000 wallet approvals (standardized minimum)
      ttl: 24 * 60 * 60 * 1000, // 24 hours TTL (force daily revalidation of approval status)
      updateAgeOnGet: false
    });

    // Log stats every 5 minutes
    setInterval(() => this.logStats(), 300000);
  }

  /**
   * Get cache key
   */
  private getCacheKey(address: string, currency: string): string {
    return `${address.toLowerCase()}-${currency.toLowerCase()}`;
  }

  /**
   * Check if wallet has approval for marketplace
   */
  async findOne(
    address: string,
    currency: string,
    marketplace: 'opensea' | 'magiceden'
  ): Promise<boolean> {
    this.stats.totalRequests++;
    const key = this.getCacheKey(address, currency);

    const entry = this.marketplaceApprovalLRU.get(key);
    if (entry) {
      this.stats.hits++;
      const hasApproval = marketplace === 'opensea'
        ? entry.openseaApproval
        : entry.magicedenApproval;

      debugLog('MarketplaceApprovalCache',
        `CACHE_HIT | ${key} | ${marketplace}=${hasApproval}`
      );

      return hasApproval;
    }

    this.stats.misses++;
    debugLog('MarketplaceApprovalCache', `CACHE_MISS | ${key}`);
    return false;
  }

  /**
   * Create or update approval entry
   */
  async upsert(
    address: string,
    currency: string,
    marketplace: 'opensea' | 'magiceden'
  ): Promise<void> {
    const key = this.getCacheKey(address, currency);

    // Get existing or create new
    let entry = this.marketplaceApprovalLRU.get(key);

    if (!entry) {
      entry = {
        address: address.toLowerCase(),
        currency: currency.toLowerCase(),
        openseaApproval: false,
        magicedenApproval: false,
        timestamp: new Date()
      };
    }

    // Update approval for marketplace
    if (marketplace === 'opensea') {
      entry.openseaApproval = true;
    } else {
      entry.magicedenApproval = true;
    }

    entry.timestamp = new Date();

    this.marketplaceApprovalLRU.set(key, entry);
    this.stats.sets++;

    debugLog('MarketplaceApprovalCache',
      `CACHE_SET | ${key} | ${marketplace}=true | ` +
      `opensea=${entry.openseaApproval} | magiceden=${entry.magicedenApproval}`
    );
  }

  /**
   * Get all approvals (for debugging/admin)
   */
  async findAll(): Promise<MarketplaceApprovalEntry[]> {
    return Array.from(this.marketplaceApprovalLRU.values());
  }

  /**
   * Clear approval for wallet (if needed for testing)
   */
  async clear(address: string, currency: string): Promise<void> {
    const key = this.getCacheKey(address, currency);
    this.marketplaceApprovalLRU.delete(key);
    debugLog('MarketplaceApprovalCache', `CACHE_CLEAR | ${key}`);
  }

  /**
   * Get cache statistics
   */
  getStats(): MarketplaceApprovalCacheStats {
    const hitRate = this.stats.totalRequests > 0
      ? ((this.stats.hits / this.stats.totalRequests) * 100).toFixed(2) + '%'
      : '0%';

    return {
      hits: this.stats.hits,
      misses: this.stats.misses,
      sets: this.stats.sets,
      totalRequests: this.stats.totalRequests,
      hitRate,
      size: this.marketplaceApprovalLRU.size,
      maxSize: this.marketplaceApprovalLRU.max
    };
  }

  /**
   * Log periodic stats
   */
  private logStats(): void {
    if (this.stats.totalRequests > 0) {
      const stats = this.getStats();
      debugLog('MarketplaceApprovalCache',
        `[5min Stats] hits=${stats.hits} | misses=${stats.misses} | ` +
        `hitRate=${stats.hitRate} | sets=${stats.sets} | ` +
        `size=${stats.size}/${stats.maxSize}`
      );
    }
  }

  /**
   * Get the internal LRU cache for debugging/monitoring
   * @internal - For use by lru-cache-sampler.ts only
   */
  public getCache(): LRUCache<string, MarketplaceApprovalEntry> {
    return this.marketplaceApprovalLRU;
  }
}

// Export singleton
export const marketplaceApprovalCache = new MarketplaceApprovalCache();

// Export stats function for health endpoint
export function getMarketplaceApprovalCacheStats(): MarketplaceApprovalCacheStats {
  return marketplaceApprovalCache.getStats();
}
