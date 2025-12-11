import { ethers } from 'ethers';
import { ERC20_MIN_ABI } from '../constants';
import { getRedisConnection } from './redis';
import { ChainValue, DEFAULT_CHAIN, getProviderForChain, normalizeChainValue } from './chains';
import { logger, ErrorCategory } from './logger';

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
  private getCacheKey(walletAddress: string, spenderAddress: string, tokenAddress: string, chain: ChainValue): string {
    return `${walletAddress}:${spenderAddress}:${tokenAddress}:${chain}`.toLowerCase();
  }

  /**
   * Check if approval exists (memory + Redis cache)
   */
  async isApproved(
    walletAddress: string,
    spenderAddress: string,
    tokenAddress: string,
    requiredAmount: string = ethers.MaxUint256.toString(),
    chain: ChainValue = DEFAULT_CHAIN
  ): Promise<boolean> {
    const resolvedChain = normalizeChainValue(chain);
    const key = this.getCacheKey(walletAddress, spenderAddress, tokenAddress, resolvedChain);

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
      console.error('[ApprovalCache] Redis error:', error);
    }

    // Cache miss - check on-chain
    try {
      const provider = getProviderForChain(resolvedChain);
      const tokenContract = new ethers.Contract(tokenAddress, ERC20_MIN_ABI, provider);
      let allowance: bigint;
      try {
        allowance = await tokenContract.allowance(walletAddress, spenderAddress);
      } catch (error: any) {
        if (error?.code === 'BAD_DATA') {
          await logger.logWarning(ErrorCategory.BLOCKCHAIN, 'Allowance decode failed, treating as zero', {
            walletAddress,
            spenderAddress,
            tokenAddress,
            chain: resolvedChain,
            errorMessage: error?.message
          });
          allowance = 0n;
        } else {
          throw error;
        }
      }
      const approved = BigInt(allowance) >= BigInt(requiredAmount);

      // Update both caches
      const entry: ApprovalCacheEntry = {
        approved,
        timestamp: Date.now(),
        allowance: allowance.toString()
      };

      await this.setApproval(walletAddress, spenderAddress, tokenAddress, entry, resolvedChain);

      return approved;
    } catch (error) {
      console.error('[ApprovalCache] On-chain check error:', error);
      return false;
    }
  }

  /**
   * Set approval status in cache
   */
  async setApproval(
    walletAddress: string,
    spenderAddress: string,
    tokenAddress: string,
    entry: ApprovalCacheEntry,
    chain: ChainValue = DEFAULT_CHAIN
  ): Promise<void> {
    const resolvedChain = normalizeChainValue(chain);
    const key = this.getCacheKey(walletAddress, spenderAddress, tokenAddress, resolvedChain);

    // Update memory cache
    this.cache.set(key, entry);

    // Update Redis cache
    try {
      const redis = getRedisConnection();
      const redisKey = this.REDIS_PREFIX + key;
      await redis.setex(redisKey, this.REDIS_TTL, JSON.stringify(entry));
    } catch (error) {
      console.error('[ApprovalCache] Failed to update Redis:', error);
    }
  }

  /**
   * Invalidate cache for a specific approval
   */
  async invalidate(walletAddress: string, spenderAddress: string, tokenAddress: string, chain: ChainValue = DEFAULT_CHAIN): Promise<void> {
    const resolvedChain = normalizeChainValue(chain);
    const key = this.getCacheKey(walletAddress, spenderAddress, tokenAddress, resolvedChain);

    // Remove from memory cache
    this.cache.delete(key);

    // Remove from Redis
    try {
      const redis = getRedisConnection();
      const redisKey = this.REDIS_PREFIX + key;
      await redis.del(redisKey);
    } catch (error) {
      console.error('[ApprovalCache] Failed to invalidate Redis cache:', error);
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
      console.error('[ApprovalCache] Failed to clear Redis cache:', error);
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
