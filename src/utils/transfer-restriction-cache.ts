import { BoundedCache } from './bounded-cache';

export interface TransferRestrictionInfo {
  contractAddress: string;
  slug: string;
  marketplace: string;
  restrictedAt: number;
  attemptCount: number;
}

class TransferRestrictionCache {
  // Cache transfer restrictions for 24 hours
  private cache: BoundedCache<string, TransferRestrictionInfo>;
  private readonly MAX_ATTEMPTS = 3;
  private readonly CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours
  private readonly BACKOFF_MULTIPLIER = 2;
  
  constructor() {
    this.cache = new BoundedCache<string, TransferRestrictionInfo>(1000, this.CACHE_TTL);
  }
  
  private getKey(contractAddress: string, marketplace: string): string {
    return `${contractAddress.toLowerCase()}-${marketplace.toLowerCase()}`;
  }
  
  isRestricted(contractAddress: string, marketplace: string): boolean {
    const key = this.getKey(contractAddress, marketplace);
    const info = this.cache.get(key);
    
    if (!info) return false;
    
    // Check if we should retry based on exponential backoff
    const now = Date.now();
    const backoffTime = this.calculateBackoffTime(info.attemptCount);
    const timeSinceRestriction = now - info.restrictedAt;
    
    // If enough time has passed, allow retry
    if (timeSinceRestriction >= backoffTime) {
      // Don't remove from cache yet, let the next attempt update it
      return false;
    }
    
    return true;
  }
  
  private calculateBackoffTime(attemptCount: number): number {
    // Start with 1 hour, then 2 hours, 4 hours, etc., max 24 hours
    const baseTime = 60 * 60 * 1000; // 1 hour
    const backoffTime = baseTime * Math.pow(this.BACKOFF_MULTIPLIER, attemptCount - 1);
    return Math.min(backoffTime, this.CACHE_TTL);
  }
  
  markAsRestricted(contractAddress: string, slug: string, marketplace: string): void {
    const key = this.getKey(contractAddress, marketplace);
    const existing = this.cache.get(key);
    
    if (existing) {
      // Increment attempt count
      this.cache.set(key, {
        ...existing,
        restrictedAt: Date.now(),
        attemptCount: Math.min(existing.attemptCount + 1, this.MAX_ATTEMPTS)
      });
    } else {
      // First restriction
      this.cache.set(key, {
        contractAddress: contractAddress.toLowerCase(),
        slug,
        marketplace: marketplace.toLowerCase(),
        restrictedAt: Date.now(),
        attemptCount: 1
      });
    }
  }
  
  removeRestriction(contractAddress: string, marketplace: string): void {
    const key = this.getKey(contractAddress, marketplace);
    this.cache.delete(key);
  }
  
  getRestrictedCollections(): TransferRestrictionInfo[] {
    const results: TransferRestrictionInfo[] = [];
    for (const key of this.cache.keys()) {
      const value = this.cache.get(key);
      if (value) {
        results.push(value);
      }
    }
    return results;
  }
  
  getRestrictionInfo(contractAddress: string, marketplace: string): TransferRestrictionInfo | null {
    const key = this.getKey(contractAddress, marketplace);
    return this.cache.get(key) || null;
  }
  
  clear(): void {
    this.cache.clear();
  }
  
  getStats(): { totalRestricted: number; byMarketplace: Record<string, number> } {
    const restricted = this.getRestrictedCollections();
    const byMarketplace: Record<string, number> = {};
    
    for (const info of restricted) {
      byMarketplace[info.marketplace] = (byMarketplace[info.marketplace] || 0) + 1;
    }
    
    return {
      totalRestricted: restricted.length,
      byMarketplace
    };
  }
}

// Export singleton instance
export const transferRestrictionCache = new TransferRestrictionCache();