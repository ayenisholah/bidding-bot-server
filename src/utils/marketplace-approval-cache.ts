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
  private cache: LRUCache<string, MarketplaceApprovalEntry>;

  // Stats tracking
  private stats = {
    hits: 0,
    misses: 0,
    sets: 0,
    totalRequests: 0
  };

  constructor() {
    this.cache = new LRUCache<string, MarketplaceApprovalEntry>({
      max: 100, // Max 100 wallet approvals (standardized)
      ttl: 0, // No TTL - persist until server restart
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

    const entry = this.cache.get(key);
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
    let entry = this.cache.get(key);

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

    this.cache.set(key, entry);
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
    return Array.from(this.cache.values());
  }

  /**
   * Clear approval for wallet (if needed for testing)
   */
  async clear(address: string, currency: string): Promise<void> {
    const key = this.getCacheKey(address, currency);
    this.cache.delete(key);
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
      size: this.cache.size,
      maxSize: this.cache.max
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
}

// Export singleton
export const marketplaceApprovalCache = new MarketplaceApprovalCache();

// Export stats function for health endpoint
export function getMarketplaceApprovalCacheStats(): MarketplaceApprovalCacheStats {
  return marketplaceApprovalCache.getStats();
}
