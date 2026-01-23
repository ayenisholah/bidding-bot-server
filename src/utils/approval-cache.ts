import { ethers } from 'ethers';
import { ERC20_MIN_ABI } from '../constants';
import { getRedisConnection } from './redis';
import { errorLog, warnLog, debugLog } from './debug-file-logger';

// Create primary provider with increased timeout (30 seconds)
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;
const provider = new ethers.AlchemyProvider('mainnet', ALCHEMY_API_KEY);

// Create fallback provider (PublicNode) for when Alchemy times out
const fallbackProvider = new ethers.JsonRpcProvider('https://ethereum.publicnode.com');

// Configure RPC timeout and retry settings
const RPC_TIMEOUT = 60000; // 60 seconds (increased from default ~10s)
const MAX_RPC_RETRIES = 3;
const RPC_RETRY_DELAY = 1000; // 1s, 2s, 4s exponential backoff

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
  private readonly MAX_CACHE_SIZE = 50000; // 50K wallet-token-marketplace combos (~7.5MB)
  private cleanupInterval: NodeJS.Timeout | null = null;

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
      const errMsg = error instanceof Error ? error.message : String(error);
      warnLog('Cache', `ApprovalCache: Redis read error | key=${key} | error=${errMsg}`);
    }

    // Cache miss - check on-chain with retry logic
    for (let attempt = 1; attempt <= MAX_RPC_RETRIES; attempt++) {
      try {
        debugLog('Balance', `[RPC] Fetching allowance attempt ${attempt}/${MAX_RPC_RETRIES} | wallet=${walletAddress.slice(0, 10)}...`);

        // Create timeout promise
        const timeoutPromise = new Promise<never>((_, reject) => {
          setTimeout(() => reject(new Error('RPC_TIMEOUT')), RPC_TIMEOUT);
        });

        // Try primary provider first (Alchemy)
        const tokenContract = new ethers.Contract(tokenAddress, ERC20_MIN_ABI, provider);
        const allowancePromise = tokenContract.allowance(walletAddress, marketplaceAddress);

        const allowance = await Promise.race([allowancePromise, timeoutPromise]);
        const approved = BigInt(allowance) >= BigInt(requiredAmount);

        // Update both caches
        const entry: ApprovalCacheEntry = {
          approved,
          timestamp: Date.now(),
          allowance: allowance.toString()
        };

        await this.setApproval(walletAddress, marketplaceAddress, tokenAddress, entry);

        debugLog('Balance', `[RPC] Success: allowance=${allowance.toString()} | wallet=${walletAddress.slice(0, 10)}...`);
        return approved;
      } catch (error: any) {
        const errorMsg = error?.message || String(error);
        const isTimeout = errorMsg.includes('TIMEOUT') || errorMsg.includes('timeout');

        // Only retry on timeout errors
        if (isTimeout && attempt < MAX_RPC_RETRIES) {
          warnLog('Balance', `[RPC] Timeout on attempt ${attempt}, retrying in ${RPC_RETRY_DELAY * attempt}ms | wallet=${walletAddress.slice(0, 10)}...`);
          await new Promise(resolve => setTimeout(resolve, RPC_RETRY_DELAY * Math.pow(2, attempt - 1)));
          continue;
        }

        // Try fallback provider on last attempt or non-timeout errors
        if (attempt === MAX_RPC_RETRIES) {
          try {
            warnLog('Balance', `[RPC] Alchemy failed, trying PublicNode fallback | wallet=${walletAddress.slice(0, 10)}...`);
            const fallbackContract = new ethers.Contract(tokenAddress, ERC20_MIN_ABI, fallbackProvider);
            const fallbackTimeoutPromise = new Promise<never>((_, reject) => {
              setTimeout(() => reject(new Error('RPC_TIMEOUT_FALLBACK')), RPC_TIMEOUT);
            });
            const fallbackAllowancePromise = fallbackContract.allowance(walletAddress, marketplaceAddress);
            const allowance = await Promise.race([fallbackAllowancePromise, fallbackTimeoutPromise]);
            const approved = BigInt(allowance) >= BigInt(requiredAmount);

            // Update caches with fallback result
            const entry: ApprovalCacheEntry = {
              approved,
              timestamp: Date.now(),
              allowance: allowance.toString()
            };
            await this.setApproval(walletAddress, marketplaceAddress, tokenAddress, entry);

            debugLog('Balance', `[RPC] Fallback success: allowance=${allowance.toString()} | wallet=${walletAddress.slice(0, 10)}...`);
            return approved;
          } catch (fallbackError: any) {
            errorLog('Balance', `[RPC] Both providers failed | wallet=${walletAddress.slice(0, 10)}...`, fallbackError);
          }
        }

        // Final failure after all retries and fallback
        if (attempt === MAX_RPC_RETRIES) {
          errorLog('Cache', `ApprovalCache: On-chain allowance check failed after retries | wallet=${walletAddress.slice(0, 10)}...`, error);
          return false;
        }
      }
    }

    return false;
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

    // Evict oldest if at capacity
    if (this.cache.size >= this.MAX_CACHE_SIZE && !this.cache.has(key)) {
      let oldestKey: string | null = null;
      let oldestTime = Date.now();

      for (const [k, v] of this.cache.entries()) {
        if (v.timestamp < oldestTime) {
          oldestTime = v.timestamp;
          oldestKey = k;
        }
      }

      if (oldestKey) this.cache.delete(oldestKey);
    }

    // Update memory cache
    this.cache.set(key, entry);

    // Update Redis cache
    try {
      const redis = getRedisConnection();
      const redisKey = this.REDIS_PREFIX + key;
      await redis.setex(redisKey, this.REDIS_TTL, JSON.stringify(entry));
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : String(error);
      warnLog('Cache', `ApprovalCache: Failed to update Redis | key=${key} | error=${errMsg}`);
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
      const errMsg = error instanceof Error ? error.message : String(error);
      warnLog('Cache', `ApprovalCache: Failed to invalidate Redis | key=${key} | error=${errMsg}`);
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
      const errMsg = error instanceof Error ? error.message : String(error);
      warnLog('Cache', `ApprovalCache: Failed to clear Redis cache | error=${errMsg}`);
    }
  }

  /**
   * Get cache statistics
   */
  getStats(): { memorySize: number; hits: number; misses: number; maxSize: number; ttl: number; estimatedMemoryBytes: number } {
    return {
      memorySize: this.cache.size,
      maxSize: this.MAX_CACHE_SIZE,
      ttl: this.CACHE_TTL,
      hits: 0, // Would need to track this
      misses: 0, // Would need to track this
      estimatedMemoryBytes: this.cache.size * 150
    };
  }

  /**
   * Start periodic cleanup (called automatically on module load)
   */
  startCleanup(): void {
    if (this.cleanupInterval) return;

    this.cleanupInterval = setInterval(() => {
      this.cleanup();
    }, 5 * 60 * 1000); // Every 5 minutes
  }

  /**
   * Periodic cleanup: Remove expired entries and enforce LRU eviction
   */
  private cleanup(): void {
    const now = Date.now();
    let removedExpired = 0;
    let removedLRU = 0;

    // Remove expired entries (24h TTL)
    for (const [key, entry] of this.cache.entries()) {
      if (now - entry.timestamp > this.CACHE_TTL) {
        this.cache.delete(key);
        removedExpired++;
      }
    }

    // LRU eviction if over limit
    if (this.cache.size > this.MAX_CACHE_SIZE) {
      const entries = Array.from(this.cache.entries())
        .sort((a, b) => a[1].timestamp - b[1].timestamp); // Oldest first

      const toRemove = this.cache.size - this.MAX_CACHE_SIZE;
      for (let i = 0; i < toRemove; i++) {
        this.cache.delete(entries[i][0]);
        removedLRU++;
      }
    }

    if (removedExpired + removedLRU > 0) {
      debugLog('Cache', `ApprovalCache cleanup | expired=${removedExpired} | lru=${removedLRU} | remaining=${this.cache.size}`);
    }
  }

  /**
   * Destroy cleanup interval (for graceful shutdown)
   */
  destroy(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
  }
}

// Export singleton instance
export const approvalCache = new ApprovalCache();
approvalCache.startCleanup(); // Auto-start cleanup on module load