import { ethers } from 'ethers';
import { ERC20_MIN_ABI } from '../constants';
import { getRedisConnection } from './redis';
import { errorLog, warnLog } from './debug-file-logger';

// Create provider instance
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
const provider = new ethers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);

interface ApprovalCacheEntry {
  approved: boolean;
  timestamp: number;
  allowance: string;
}

class ApprovalCache {
  private cache = new Map<string, ApprovalCacheEntry>();
  private readonly CACHE_TTL = 24 * 60 * 60 * 1000; // 1 day
  private readonly REDIS_PREFIX = '{approval}';
  private readonly REDIS_TTL = 86400; // 1 day in seconds

  /**
   * Get cache key for wallet/marketplace/token combination
   */
  private getCacheKey(walletAddress: string, marketplaceAddress: string, tokenAddress: string): string {
    return `${walletAddress}:${marketplaceAddress}:${tokenAddress}`.toLowerCase();
  }

  /**
   * Check if approval exists (memory + Redis cache)
   */
  async isApproved(
    walletAddress: string,
    marketplaceAddress: string,
    tokenAddress: string,
    requiredAmount: string = ethers.MaxUint256.toString()
  ): Promise<boolean> {
    const key = this.getCacheKey(walletAddress, marketplaceAddress, tokenAddress);

    // Check memory cache first
    const memoryCache = this.cache.get(key);
    if (memoryCache && Date.now() - memoryCache.timestamp < this.CACHE_TTL) {
      return BigInt(memoryCache.allowance) >= BigInt(requiredAmount);
    }

    // Check Redis cache
    try {
      const redis = getRedisConnection();
      const redisKey = this.REDIS_PREFIX + key;
      const redisData = await redis.get(redisKey);
      if (redisData) {
        const entry: ApprovalCacheEntry = JSON.parse(redisData);
        // Update memory cache from Redis
        this.cache.set(key, entry);
        return BigInt(entry.allowance) >= BigInt(requiredAmount);
      }
    } catch (error) {
      warnLog('Cache', `ApprovalCache: Redis read error | key=${key}`);
    }

    // Cache miss - check on-chain
    try {
      const tokenContract = new ethers.Contract(tokenAddress, ERC20_MIN_ABI, provider);
      const allowance = await tokenContract.allowance(walletAddress, marketplaceAddress);
      const approved = BigInt(allowance) >= BigInt(requiredAmount);

      // Update both caches
      const entry: ApprovalCacheEntry = {
        approved,
        timestamp: Date.now(),
        allowance: allowance.toString()
      };

      await this.setApproval(walletAddress, marketplaceAddress, tokenAddress, entry);

      return approved;
    } catch (error) {
      errorLog('Cache', `ApprovalCache: On-chain allowance check failed | wallet=${walletAddress.slice(0, 10)}...`, error);
      return false;
    }
  }

  /**
   * Set approval status in cache
   */
  async setApproval(
    walletAddress: string,
    marketplaceAddress: string,
    tokenAddress: string,
    entry: ApprovalCacheEntry
  ): Promise<void> {
    const key = this.getCacheKey(walletAddress, marketplaceAddress, tokenAddress);

    // Update memory cache
    this.cache.set(key, entry);

    // Update Redis cache
    try {
      const redis = getRedisConnection();
      const redisKey = this.REDIS_PREFIX + key;
      await redis.setex(redisKey, this.REDIS_TTL, JSON.stringify(entry));
    } catch (error) {
      warnLog('Cache', `ApprovalCache: Failed to update Redis | key=${key}`);
    }
  }

  /**
   * Invalidate cache for a specific approval
   */
  async invalidate(walletAddress: string, marketplaceAddress: string, tokenAddress: string): Promise<void> {
    const key = this.getCacheKey(walletAddress, marketplaceAddress, tokenAddress);

    // Remove from memory cache
    this.cache.delete(key);

    // Remove from Redis
    try {
      const redis = getRedisConnection();
      const redisKey = this.REDIS_PREFIX + key;
      await redis.del(redisKey);
    } catch (error) {
      warnLog('Cache', `ApprovalCache: Failed to invalidate Redis | key=${key}`);
    }
  }

  /**
   * Clear all cached approvals (for testing/maintenance)
   */
  async clearAll(): Promise<void> {
    this.cache.clear();

    try {
      const redis = getRedisConnection();
      const keys = await redis.keys(this.REDIS_PREFIX + '*');
      if (keys.length > 0) {
        await redis.del(...keys);
      }
    } catch (error) {
      warnLog('Cache', 'ApprovalCache: Failed to clear Redis cache');
    }
  }

  /**
   * Get cache statistics
   */
  getStats(): { memorySize: number; hits: number; misses: number } {
    return {
      memorySize: this.cache.size,
      hits: 0, // Would need to track this
      misses: 0 // Would need to track this
    };
  }
}

// Export singleton instance
export const approvalCache = new ApprovalCache();