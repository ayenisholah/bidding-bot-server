/**
 * ERC20 Token Allowance Cache
 *
 * Caches numeric allowance amounts for wallet-marketplace-token combinations.
 * Makes on-chain RPC calls to verify allowances and caches results for 24 hours.
 *
 * NOT to be confused with marketplaceApprovalCache which tracks high-level
 * approval status flags (boolean) rather than actual allowance amounts.
 */
import { ethers } from 'ethers';
import { LRUCache } from 'lru-cache';
import { ERC20_MIN_ABI } from '../constants';
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
  private approvalEntriesLRU: LRUCache<string, ApprovalCacheEntry>;
  private readonly CACHE_TTL = 24 * 60 * 60 * 1000; // 1 day
  private readonly MAX_CACHE_SIZE = 50000; // 50K wallet-token-marketplace combos (~66KB)

  constructor() {
    this.approvalEntriesLRU = new LRUCache<string, ApprovalCacheEntry>({
      max: this.MAX_CACHE_SIZE,
      ttl: this.CACHE_TTL,
      updateAgeOnGet: true,
      updateAgeOnHas: true,
    });
  }

  /**
   * Get cache key for wallet/marketplace/token combination
   */
  private getCacheKey(walletAddress: string, marketplaceAddress: string, tokenAddress: string): string {
    return `${walletAddress}:${marketplaceAddress}:${tokenAddress}`.toLowerCase();
  }

  /**
   * Check if approval exists (LRU cache)
   */
  async isApproved(
    walletAddress: string,
    marketplaceAddress: string,
    tokenAddress: string,
    requiredAmount: string = ethers.MaxUint256.toString()
  ): Promise<boolean> {
    const key = this.getCacheKey(walletAddress, marketplaceAddress, tokenAddress);

    // Check LRU cache
    const cached = this.approvalEntriesLRU.get(key);
    if (cached) {
      return BigInt(cached.allowance) >= BigInt(requiredAmount);
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
    this.approvalEntriesLRU.set(key, entry);
  }

  /**
   * Invalidate cache for a specific approval
   */
  async invalidate(walletAddress: string, marketplaceAddress: string, tokenAddress: string): Promise<void> {
    const key = this.getCacheKey(walletAddress, marketplaceAddress, tokenAddress);
    this.approvalEntriesLRU.delete(key);
  }

  /**
   * Clear all cached approvals (for testing/maintenance)
   */
  async clearAll(): Promise<void> {
    this.approvalEntriesLRU.clear();
  }

  /**
   * Get cache statistics
   */
  getStats(): { size: number; maxSize: number; ttl: number; estimatedMemoryBytes: number } {
    return {
      size: this.approvalEntriesLRU.size,
      maxSize: this.MAX_CACHE_SIZE,
      ttl: this.CACHE_TTL,
      estimatedMemoryBytes: this.approvalEntriesLRU.size * 66, // ~66 bytes per entry
    };
  }

  /**
   * Get the internal LRU cache for debugging/monitoring
   * @internal - For use by lru-cache-sampler.ts only
   */
  public getCache(): LRUCache<string, ApprovalCacheEntry> {
    return this.approvalEntriesLRU;
  }
}

// Export singleton instance
export const approvalCache = new ApprovalCache();