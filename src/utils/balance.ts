import { DistributedLockManager } from "./lock";
import { API_KEY } from "..";
import { limiter, axiosInstance } from "../init";
import { BASE_URL } from "../constants";
import { warnLog, errorLog, debugLog } from "./debug-file-logger";
import { apiDedup } from "./api-dedup";
import {
  balanceLRU,
  trackBalanceHit,
  trackBalanceMiss
} from "../marketplace/opensea/cache";

const WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".toLowerCase();

// Debug logging controlled by single environment variable
const DEBUG = process.env.DEBUG === 'true';

function truncateAddress(address: string): string {
  return `${address.slice(0, 6)}...${address.slice(-4)}`;
}

interface BalanceResponse {
  data: {
    addressBalances: Array<{
      currency: {
        contractAddress: string;
        chain: {
          identifier: string;
        };
      };
      quantity: string;
    }>;
  };
}

/**
 * Fetch WETH balance using OpenSea's GraphQL API.
 * This is faster and more reliable than Alchemy RPC calls.
 */
async function fetchWethBalanceGraphQL(walletAddress: string): Promise<number> {
  // Note: Removed "GraphQL fetch starting/complete" logs - was noise (35K+ lines/day)

  const query = {
    operationName: "WethBalanceQuery",
    query: `query WethBalanceQuery($address: Address!) {
      addressBalances(address: $address) {
        currency {
          contractAddress
          chain {
            identifier
          }
        }
        quantity
      }
    }`,
    variables: { address: walletAddress }
  };

  const { data } = await apiDedup(
    `opensea-weth-balance-${walletAddress.toLowerCase()}`,
    () => limiter.schedule({ priority: 0 }, () => axiosInstance.post<BalanceResponse>(
      `${BASE_URL}/opensea/graphql`,
      query,
      {
        headers: {
          'content-type': 'application/json',
          'x-api-key': API_KEY
        }
      }
    )),
    { category: 'Balance' }
  );

  const wethBalance = data.data.addressBalances.find(
    (bal) =>
      bal.currency.contractAddress.toLowerCase() === WETH_ADDRESS &&
      bal.currency.chain.identifier === "ethereum"
  );

  const balance = wethBalance ? Number(wethBalance.quantity) : 0;
  return balance;
}

/**
 * Invalidate the balance cache for a specific wallet/token combination.
 * Called when we receive a balance error from marketplace APIs to ensure
 * the next balance check fetches fresh data.
 */
export async function invalidateBalanceCache(walletAddress: string, tokenAddress: string): Promise<void> {
  const normalizedAddress = walletAddress.toLowerCase();

  try {
    balanceLRU.delete(normalizedAddress);
    debugLog('Balance', `Invalidated LRU cache | wallet=${walletAddress.slice(0, 10)}... | token=${tokenAddress.slice(0, 10)}...`);
  } catch (error) {
    errorLog('Balance', `Failed to invalidate cache | wallet=${walletAddress.slice(0, 10)}...`, error);
  }
}

export async function fetchERC20Balance(walletAddress: string, _tokenAddress: string): Promise<number> {
  const CACHE_EXPIRY_SECONDS = 300;  // 5 minutes
  const normalizedAddress = walletAddress.toLowerCase();

  // Check LRU cache (< 1ms)
  const lruCached = balanceLRU.get(normalizedAddress);
  if (lruCached !== undefined) {
    trackBalanceHit();
    debugLog('Performance', `[OS-Balance] LRU_HIT (<1ms) | wallet=${walletAddress.slice(0,10)}... | balance=${lruCached}`);
    return lruCached;
  }

  // Cache miss - fetch from API (800-1500ms)
  trackBalanceMiss();
  debugLog('Performance', `[OS-Balance] CACHE_MISS | wallet=${walletAddress.slice(0,10)}... | fetching...`);
  const fetchStart = Date.now();

  const lockManager = new DistributedLockManager({
    lockPrefix: 'erc20_balance:lock:',
    defaultTTLSeconds: CACHE_EXPIRY_SECONDS
  });

  const lockKey = `{erc20_${walletAddress}}_weth`;

  try {
    const result = await lockManager.withLock(
      lockKey,
      async () => {
        // Double-check LRU after acquiring lock
        const lruCachedAfterLock = balanceLRU.get(normalizedAddress);
        if (lruCachedAfterLock !== undefined) {
          return lruCachedAfterLock;
        }

        try {
          // Use OpenSea GraphQL API with deduplication
          const balance = await fetchWethBalanceGraphQL(walletAddress);

          // Store in LRU cache (5 minute TTL)
          balanceLRU.set(normalizedAddress, balance);

          const fetchTime = Date.now() - fetchStart;
          debugLog('Performance', `[OS-Balance] API_FETCH | wallet=${walletAddress.slice(0,10)}... | balance=${balance} | fetchTime=${fetchTime}ms`);

          return balance;
        } catch (error) {
          errorLog('Balance', `Error fetching WETH balance via GraphQL | wallet=${walletAddress.slice(0, 10)}...`, error);
          throw error;
        }
      }
    );

    return result || 0;
  } catch (error) {
    // If we get a network error or lock error, return 0
    errorLog('Balance', `Failed to fetch WETH balance | wallet=${walletAddress.slice(0, 10)}...`, error);
    return 0;
  }
}

export async function fetchERC20BalanceFresh(walletAddress: string, _tokenAddress: string): Promise<number> {
  const CACHE_EXPIRY_SECONDS = 60;  // 1 minute
  const normalizedAddress = walletAddress.toLowerCase();
  const startTime = Date.now(); // Used for error logging (elapsed time on failures)

  // Check LRU cache first (optional for "fresh" fetch)
  const lruCached = balanceLRU.get(normalizedAddress);
  if (lruCached !== undefined) {
    trackBalanceHit();
    debugLog('Performance', `[OS-Balance-Fresh] LRU_HIT | wallet=${walletAddress.slice(0,10)}... | balance=${lruCached}`);
    return lruCached;
  }

  // Cache miss - fetch fresh from API
  trackBalanceMiss();

  // Create promise with timeout (10 seconds to account for network latency)
  const timeoutPromise = new Promise<number>((_, reject) => {
    setTimeout(() => reject(new Error('Balance fetch timeout after 10 seconds')), 10000);
  });

  const fetchPromise = async (): Promise<number> => {
    const lockManager = new DistributedLockManager({
      lockPrefix: 'erc20_balance:lock:',
      defaultTTLSeconds: CACHE_EXPIRY_SECONDS
    });

    const lockKey = `{erc20_${walletAddress}}_weth`;

    try {
      const result = await lockManager.withLock(
        lockKey,
        async () => {
          try {
            // Use OpenSea GraphQL API with deduplication
            const balance = await fetchWethBalanceGraphQL(walletAddress);

            // Store in LRU cache (5 minute TTL)
            balanceLRU.set(normalizedAddress, balance);

            return balance;
          } catch (error) {
            errorLog('Balance', `Error fetching fresh WETH balance via GraphQL | wallet=${walletAddress.slice(0, 10)}...`, error);
            throw error;
          }
        }
      );

      return result || 0;
    } catch (error: any) {
      errorLog('Balance', `Lock/fetch error | wallet=${walletAddress.slice(0, 10)}...`, error);
      throw error;
    }
  };

  try {
    // Race between fetch and timeout
    const balance = await Promise.race([fetchPromise(), timeoutPromise]);
    return balance;
  } catch (error) {
    const elapsed = Date.now() - startTime;
    const errorMsg = error instanceof Error ? error.message : 'Unknown error';
    warnLog('Balance', `[fetchERC20BalanceFresh] Failed after ${elapsed}ms: ${errorMsg} | wallet=${walletAddress.slice(0, 10)}...`);

    // Try LRU cache as fallback
    const fallbackBalance = balanceLRU.get(normalizedAddress);
    if (fallbackBalance !== undefined) {
      if (DEBUG) {
        debugLog('Balance', `[Fresh] LRU cache fallback | wallet=${truncateAddress(walletAddress)} | balance=${fallbackBalance} ETH`);
      }
      return fallbackBalance;
    }

    // Return 0 as final fallback
    warnLog('Balance', `[fetchERC20BalanceFresh] No cached balance available, returning 0.00 | wallet=${walletAddress.slice(0, 10)}...`);
    return 0;
  }
}
