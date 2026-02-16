import { errorLog } from "./debug-file-logger";
import { ChainValue, DEFAULT_CHAIN, getProviderForChain, getCurrencyDecimals } from "./chains";
import {
  balanceLRU,
  trackBalanceHit,
  trackBalanceMiss
} from "../marketplace/opensea/cache";
import { ethers } from "ethers";

// In-flight balance fetch tracking (Phase 4 migration - replaces distributed locks)
const balanceFetchInFlight = new Map<string, Promise<number>>();

/**
 * Fetch balance using Alchemy RPC eth_call with Contract.balanceOf().
 * Supports all chains with chain-specific providers and token addresses.
 * @param walletAddress - Wallet address to check balance for
 * @param tokenAddress - ERC20 token contract address
 * @param chain - Chain to query (ethereum, base, megaeth, abstract, etc.)
 * @returns Token balance as Wei string (e.g., "1000000000000000000" for 1 token)
 */
async function fetchBalanceFromAlchemy(
  walletAddress: string,
  tokenAddress: string,
  chain: ChainValue
): Promise<string> {
  const provider = getProviderForChain(chain);

  const contract = new ethers.Contract(
    tokenAddress,
    ['function balanceOf(address) view returns (uint256)'],
    provider
  );

  const balance: bigint = await contract.balanceOf(walletAddress);
  return balance.toString();
}

/**
 * Fetch balance with retry logic and exponential backoff.
 * Retries up to 3 times with delays of 1s, 2s, 4s.
 * @param walletAddress - Wallet address to check balance for
 * @param tokenAddress - ERC20 token contract address
 * @param chain - Chain to query
 * @param maxRetries - Maximum number of retry attempts (default: 3)
 * @returns Token balance as Wei string
 * @throws Error if all retries fail
 */
async function fetchBalanceWithRetry(
  walletAddress: string,
  tokenAddress: string,
  chain: ChainValue,
  maxRetries: number = 3
): Promise<string> {
  let lastError: Error | null = null;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fetchBalanceFromAlchemy(walletAddress, tokenAddress, chain);
    } catch (error: any) {
      lastError = error;
      if (attempt < maxRetries - 1) {
        // Exponential backoff: 1s, 2s, 4s
        const delay = Math.pow(2, attempt) * 1000;
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  throw lastError || new Error('Failed to fetch balance after retries');
}

/**
 * Invalidate the balance cache for a specific wallet/token/chain combination.
 * Called when we receive a balance error from marketplace APIs to ensure
 * the next balance check fetches fresh data.
 *
 * Note: If chain/tokenAddress not provided, this clears all cached balances for the wallet
 * by iterating through all cache keys (less efficient but handles backwards compatibility).
 */
export async function invalidateBalanceCache(
  walletAddress: string,
  tokenAddress?: string,
  chain?: ChainValue
): Promise<void> {
  const normalizedAddress = walletAddress.toLowerCase();

  try {
    if (tokenAddress && chain) {
      // Specific cache key (efficient)
      const normalizedToken = tokenAddress.toLowerCase();
      const cacheKey = `${normalizedAddress}:${chain}:${normalizedToken}`;
      balanceLRU.delete(cacheKey);
    } else {
      // Clear all balances for this wallet (backwards compatibility)
      // Iterate through all cache keys and delete matching wallet entries
      const keysToDelete: string[] = [];
      for (const key of balanceLRU.keys()) {
        if (key.startsWith(normalizedAddress + ':')) {
          keysToDelete.push(key);
        }
      }
      keysToDelete.forEach(key => balanceLRU.delete(key));
    }
  } catch (error) {
    errorLog('Balance', `Failed to invalidate cache | wallet=${walletAddress.slice(0, 10)}...`, error);
  }
}

/**
 * Fetch ERC20 token balance for a wallet on a specific chain.
 * Uses Alchemy RPC for multichain support with LRU caching and in-flight request deduplication.
 *
 * @param walletAddress - Wallet address to check balance for
 * @param tokenAddress - ERC20 token contract address
 * @param chain - Chain to query (ethereum, base, megaeth, abstract, etc.)
 * @returns Token balance in ETH (human-readable, e.g., 0.5 for 0.5 WETH)
 */
export async function fetchERC20Balance(
  walletAddress: string,
  tokenAddress: string,
  chain: ChainValue = DEFAULT_CHAIN
): Promise<number> {
  const normalizedAddress = walletAddress.toLowerCase();
  const normalizedToken = tokenAddress.toLowerCase();

  // Include chain in cache key to prevent cross-chain pollution
  const cacheKey = `${normalizedAddress}:${chain}:${normalizedToken}`;

  // Check LRU cache (< 1ms)
  const lruCached = balanceLRU.get(cacheKey);
  if (lruCached !== undefined) {
    trackBalanceHit();
    return lruCached;
  }

  // Cache miss - fetch from Alchemy RPC
  trackBalanceMiss();

  // Use in-flight Map to prevent duplicate concurrent fetches
  let inFlightPromise = balanceFetchInFlight.get(cacheKey);

  if (!inFlightPromise) {
    // Create new fetch operation
    inFlightPromise = (async () => {
      try {
        // Double-check LRU after starting fetch
        const lruCachedAfterStart = balanceLRU.get(cacheKey);
        if (lruCachedAfterStart !== undefined) {
          return lruCachedAfterStart;
        }

        // Fetch balance via Alchemy RPC with retry
        const balanceWei = await fetchBalanceWithRetry(walletAddress, tokenAddress, chain);

        // Convert Wei to ETH using chain-specific decimals
        const decimals = getCurrencyDecimals(chain, 'WETH') || 18; // Default to 18 if not found
        const balanceEth = Number(balanceWei) / Math.pow(10, decimals);

        // Cache the result
        balanceLRU.set(cacheKey, balanceEth);

        return balanceEth;

      } catch (error) {
        errorLog('Balance',
          `Error fetching balance via Alchemy RPC | chain=${chain} | wallet=${walletAddress.slice(0, 10)}... | token=${tokenAddress.slice(0, 10)}...`,
          error
        );
        throw error;
      } finally {
        // Remove from in-flight tracking
        balanceFetchInFlight.delete(cacheKey);
      }
    })();

    balanceFetchInFlight.set(cacheKey, inFlightPromise);
  }

  try {
    // Wait for fetch to complete (either new or in-progress)
    const result = await inFlightPromise;
    return result || 0;
  } catch (error) {
    // If we get a network error and no cache, return 0
    errorLog('Balance',
      `Failed to fetch balance (no cached fallback) | chain=${chain} | wallet=${walletAddress.slice(0, 10)}...`,
      error
    );
    return 0;
  }
}
