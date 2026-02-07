import { warnLog, errorLog} from "./debug-file-logger";
import {
  balanceLRU,
  trackBalanceHit,
  trackBalanceMiss
} from "../marketplace/opensea/cache";
import { mainProvider, fallbackProvider } from "./provider";
import { Contract } from "ethers";

const WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".toLowerCase();

// ERC20 balanceOf ABI
const ERC20_BALANCE_ABI = [
  "function balanceOf(address owner) view returns (uint256)",
];

// In-flight balance fetch tracking (Phase 4 migration - replaces distributed locks)
const balanceFetchInFlight = new Map<string, Promise<number>>();

/**
 * Fetch WETH balance using Alchemy RPC eth_call.
 * More reliable than OpenSea GraphQL API, prevents false balance=0 issues.
 *
 * @returns WETH balance in ETH (e.g., 0.5 for 0.5 WETH)
 */
async function fetchWethBalanceAlchemy(walletAddress: string): Promise<number> {
  const RPC_TIMEOUT = 10000; // 10 seconds
  const MAX_RETRIES = 3;
  const RETRY_DELAYS = [1000, 2000, 4000]; // Exponential backoff

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      // Create timeout promise
      const timeoutPromise = new Promise<never>((_, reject) => {
        setTimeout(() => reject(new Error('RPC_TIMEOUT')), RPC_TIMEOUT);
      });

      // Create WETH contract instance with primary provider
      const wethContract = new Contract(
        WETH_ADDRESS,
        ERC20_BALANCE_ABI,
        mainProvider
      );

      // Fetch balance with timeout race
      const balancePromise = wethContract.balanceOf(walletAddress);
      const balanceRaw = await Promise.race([balancePromise, timeoutPromise]);

      // Convert from wei to ETH (BigInt division)
      const balanceWei = BigInt(balanceRaw.toString());
      const balance = Number(balanceWei) / 1e18;

      return balance;

    } catch (error: any) {
      const errorMsg = error.message || 'Unknown error';

      errorLog('Balance',
        `Alchemy RPC attempt ${attempt}/${MAX_RETRIES} failed | wallet=${walletAddress.slice(0, 10)}... | error=${errorMsg}`,
        error
      );

      // Retry with exponential backoff
      if (attempt < MAX_RETRIES) {
        const delay = RETRY_DELAYS[attempt - 1];
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }

      // Final attempt: try fallback provider (PublicNode)
      if (attempt === MAX_RETRIES) {
        try {

          const fallbackContract = new Contract(
            WETH_ADDRESS,
            ERC20_BALANCE_ABI,
            fallbackProvider
          );

          const fallbackBalance = await fallbackContract.balanceOf(walletAddress);
          const balanceWei = BigInt(fallbackBalance.toString());
          const balance = Number(balanceWei) / 1e18;

          return balance;

        } catch (fallbackError) {
          errorLog('Balance',
            `Fallback provider also failed | wallet=${walletAddress.slice(0, 10)}...`,
            fallbackError
          );
          throw fallbackError;
        }
      }
    }
  }

  // This should never be reached due to throw in final catch
  throw new Error('All balance fetch attempts failed');
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
  } catch (error) {
    errorLog('Balance', `Failed to invalidate cache | wallet=${walletAddress.slice(0, 10)}...`, error);
  }
}

export async function fetchERC20Balance(walletAddress: string, _tokenAddress: string): Promise<number> {
  const normalizedAddress = walletAddress.toLowerCase();

  // Check LRU cache (< 1ms)
  const lruCached = balanceLRU.get(normalizedAddress);
  if (lruCached !== undefined) {
    trackBalanceHit();
    return lruCached;
  }

  // Cache miss - fetch from Alchemy RPC
  trackBalanceMiss();
  const fetchStart = Date.now();

  // Use in-flight Map to prevent duplicate concurrent fetches
  let inFlightPromise = balanceFetchInFlight.get(normalizedAddress);

  if (!inFlightPromise) {
    // Create new fetch operation
    inFlightPromise = (async () => {
      try {
        // Double-check LRU after starting fetch
        const lruCachedAfterStart = balanceLRU.get(normalizedAddress);
        if (lruCachedAfterStart !== undefined) {
          return lruCachedAfterStart;
        }

        try {
          // Use Alchemy RPC with retry logic and fallback
          const balance = await fetchWethBalanceAlchemy(walletAddress);

          // Defensive caching: validate balance before evicting old value
          if (balance === 0) {
            warnLog('Balance',
              `[DEFENSIVE CACHE] Alchemy returned balance=0 | wallet=${walletAddress.slice(0,10)}... | ` +
              `This may indicate: (1) truly empty wallet, (2) RPC error, or (3) API bug. ` +
              `Old cached value preserved if available.`
            );

            // Only cache 0 if there's no existing cached value
            const existingCached = balanceLRU.get(normalizedAddress);
            if (existingCached === undefined) {
              balanceLRU.set(normalizedAddress, balance);
            } else {
              return existingCached; // Return old value, don't evict
            }
          } else {
            // Valid non-zero balance - safe to cache
            balanceLRU.set(normalizedAddress, balance);
          }

          const fetchTime = Date.now() - fetchStart;

          return balance;

        } catch (error) {
          errorLog('Balance',
            `Error fetching WETH balance via Alchemy RPC | wallet=${walletAddress.slice(0, 10)}...`,
            error
          );

          // On error, return cached value if available (defensive fallback)
          const cachedFallback = balanceLRU.get(normalizedAddress);
          if (cachedFallback !== undefined) {
            warnLog('Balance',
              `[DEFENSIVE CACHE] Alchemy RPC error, using stale cached value=${cachedFallback} | ` +
              `wallet=${walletAddress.slice(0,10)}...`
            );
            return cachedFallback;
          }

          throw error;
        }
      } finally {
        // Remove from in-flight tracking
        balanceFetchInFlight.delete(normalizedAddress);
      }
    })();

    balanceFetchInFlight.set(normalizedAddress, inFlightPromise);
  }

  try {
    // Wait for fetch to complete (either new or in-progress)
    const result = await inFlightPromise;
    return result || 0;
  } catch (error) {
    // If we get a network error and no cache, return 0
    errorLog('Balance',
      `Failed to fetch WETH balance (no cached fallback) | wallet=${walletAddress.slice(0, 10)}...`,
      error
    );
    return 0;
  }
}
