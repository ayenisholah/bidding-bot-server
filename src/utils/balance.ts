import { DistributedLockManager } from "./lock";
import redisClient from "./redis";
import { RED, RESET, API_KEY } from "..";
import { limiter, axiosInstance } from "../init";
import { BASE_URL } from "../constants";
import { warnLog, errorLog } from "./debug-file-logger";

const getRedis = () => redisClient.getClient()

const WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".toLowerCase();

// Debug logging controlled by single environment variable
const DEBUG = process.env.DEBUG === 'true';

function formatWeiToEth(wei: number): string {
  return (wei / 1e18).toFixed(6);
}

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
  const startTime = Date.now();

  if (DEBUG) {
    console.log(`[WETH Balance] GraphQL fetch starting: wallet=${truncateAddress(walletAddress)}`);
  }

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

  const { data } = await limiter.schedule({ priority: 0 }, () => axiosInstance.post<BalanceResponse>(
    `${BASE_URL}/opensea/graphql`,
    query,
    {
      headers: {
        'content-type': 'application/json',
        'x-api-key': API_KEY
      }
    }
  ));

  const wethBalance = data.data.addressBalances.find(
    (bal) =>
      bal.currency.contractAddress.toLowerCase() === WETH_ADDRESS &&
      bal.currency.chain.identifier === "ethereum"
  );

  const balance = wethBalance ? Number(wethBalance.quantity) : 0;

  if (DEBUG) {
    const elapsed = Date.now() - startTime;
    console.log(`[WETH Balance] GraphQL fetch complete: wallet=${truncateAddress(walletAddress)}, balance=${balance} ETH, elapsed=${elapsed}ms`);
  }

  return balance;
}

/**
 * Invalidate the balance cache for a specific wallet/token combination.
 * Called when we receive a balance error from marketplace APIs to ensure
 * the next balance check fetches fresh data.
 */
export async function invalidateBalanceCache(walletAddress: string, tokenAddress: string): Promise<void> {
  const cacheKey = `{erc20_balance:${walletAddress}}:${tokenAddress}`;
  const staleCacheKey = `{erc20_balance_stale:${walletAddress}}:${tokenAddress}`;

  try {
    const redis = getRedis();
    if (redis) {
      await redis.del(cacheKey);
      await redis.del(staleCacheKey);
      console.log(`[Balance] Invalidated cache for ${walletAddress.slice(0, 10)}.../${tokenAddress.slice(0, 10)}...`);
    }
  } catch (error) {
    warnLog('Balance', `Failed to invalidate cache | wallet=${walletAddress.slice(0, 10)}...`);
  }
}

export async function fetchERC20Balance(walletAddress: string, _tokenAddress: string): Promise<number> {
  const CACHE_EXPIRY_SECONDS = 60;
  const STALE_CACHE_EXPIRY_SECONDS = 3600; // Keep stale cache for 1 hour as fallback
  const cacheKey = `{erc20_balance:${walletAddress}}:weth`;
  const staleCacheKey = `{erc20_balance_stale:${walletAddress}}:weth`;

  // Try to get fresh cached balance first
  try {
    const redis = getRedis();
    if (redis) {
      const cachedBalance = await redis.get(cacheKey);
      if (cachedBalance) {
        if (DEBUG) {
          console.log(`[WETH Balance] Cache hit (fresh): wallet=${truncateAddress(walletAddress)}, balance=${cachedBalance} ETH`);
        }
        return Number(cachedBalance);
      }
    }
  } catch (error) {
    warnLog('Balance', `Redis not available for cache lookup | wallet=${walletAddress.slice(0, 10)}...`);
  }

  const lockManager = new DistributedLockManager({
    lockPrefix: 'erc20_balance:lock:',
    defaultTTLSeconds: CACHE_EXPIRY_SECONDS
  });

  const lockKey = `{erc20_${walletAddress}}_weth`;

  try {
    const result = await lockManager.withLock(
      lockKey,
      async () => {
        // Check cache again after acquiring lock
        try {
          const redis = getRedis();
          if (redis) {
            const cachedBalanceAfterLock = await redis.get(cacheKey);
            if (cachedBalanceAfterLock) {
              if (DEBUG) {
                console.log(`[WETH Balance] Cache hit (after lock): wallet=${truncateAddress(walletAddress)}, balance=${cachedBalanceAfterLock} ETH`);
              }
              return Number(cachedBalanceAfterLock);
            }
          }
        } catch (error) {
          warnLog('Balance', `Redis not available for cache lookup after lock | wallet=${walletAddress.slice(0, 10)}...`);
        }

        try {
          // Use OpenSea GraphQL API instead of Alchemy RPC
          const balance = await fetchWethBalanceGraphQL(walletAddress);

          // Store in both fresh and stale cache
          try {
            const redis = getRedis();
            if (redis) {
              await redis.set(cacheKey, balance.toString(), 'EX', CACHE_EXPIRY_SECONDS);
              await redis.set(staleCacheKey, balance.toString(), 'EX', STALE_CACHE_EXPIRY_SECONDS);
            }
          } catch (error) {
            warnLog('Balance', `Failed to cache balance | wallet=${walletAddress.slice(0, 10)}...`);
          }

          if (DEBUG) {
            console.log(`[WETH Balance] Fresh fetch success: wallet=${truncateAddress(walletAddress)}, balance=${balance} ETH`);
          }

          return balance;
        } catch (error) {
          errorLog('Balance', `Error fetching WETH balance via GraphQL | wallet=${walletAddress.slice(0, 10)}...`, error);
          throw error;
        }
      }
    );

    return result || 0;
  } catch (error: any) {
    // If we get a network error or lock error, try to use stale cache
    warnLog('Balance', `Failed to fetch fresh WETH balance, checking stale cache | wallet=${walletAddress.slice(0, 10)}...`);

    let staleBalance = null;
    try {
      const redis = getRedis();
      if (redis) {
        staleBalance = await redis.get(staleCacheKey);
      }
    } catch (error) {
      warnLog('Balance', `Failed to get stale cache | wallet=${walletAddress.slice(0, 10)}...`);
    }
    if (staleBalance) {
      if (DEBUG) {
        console.log(`[WETH Balance] Stale cache fallback: wallet=${truncateAddress(walletAddress)}, balance=${staleBalance} ETH`);
      }
      return Number(staleBalance);
    }

    // If no stale cache available, log error and return 0
    warnLog('Balance', `No cached WETH balance available | wallet=${walletAddress.slice(0, 10)}...`);
    return 0;
  }
}

export async function fetchERC20BalanceFresh(walletAddress: string, _tokenAddress: string): Promise<number> {
  const CACHE_EXPIRY_SECONDS = 60;
  const STALE_CACHE_EXPIRY_SECONDS = 3600; // Keep stale cache for 1 hour as fallback
  const cacheKey = `{erc20_balance:${walletAddress}}:weth`;
  const staleCacheKey = `{erc20_balance_stale:${walletAddress}}:weth`;
  const startTime = Date.now();

  // First try to get cached value
  try {
    const redis = getRedis();
    if (redis) {
      const cachedBalance = await redis.get(cacheKey);
      if (cachedBalance) {
        if (DEBUG) {
          console.log(`[WETH Balance Fresh] Cache hit: wallet=${truncateAddress(walletAddress)}, balance=${cachedBalance} ETH`);
        }
        return Number(cachedBalance);
      }
    }
  } catch (error) {
    warnLog('Balance', `[fetchERC20BalanceFresh] Redis error when checking cache | wallet=${walletAddress.slice(0, 10)}...`);
  }

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
            // Use OpenSea GraphQL API instead of Alchemy RPC
            const balance = await fetchWethBalanceGraphQL(walletAddress);

            // Store in both fresh and stale cache
            try {
              const redis = getRedis();
              if (redis) {
                await redis.set(cacheKey, balance.toString(), 'EX', CACHE_EXPIRY_SECONDS);
                await redis.set(staleCacheKey, balance.toString(), 'EX', STALE_CACHE_EXPIRY_SECONDS);
              }
            } catch (error) {
              warnLog('Balance', `Failed to cache balance | wallet=${walletAddress.slice(0, 10)}...`);
            }

            if (DEBUG) {
              const elapsed = Date.now() - startTime;
              console.log(`[WETH Balance Fresh] Fresh fetch success: wallet=${truncateAddress(walletAddress)}, balance=${balance} ETH, elapsed=${elapsed}ms`);
            }

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
    warnLog('Balance', `[fetchERC20BalanceFresh] Failed after ${elapsed}ms: ${errorMsg}, checking stale cache | wallet=${walletAddress.slice(0, 10)}...`);

    // Try stale cache
    try {
      const redis = getRedis();
      if (redis) {
        const staleBalance = await redis.get(staleCacheKey);
        if (staleBalance) {
          if (DEBUG) {
            console.log(`[WETH Balance Fresh] Stale cache fallback: wallet=${truncateAddress(walletAddress)}, balance=${staleBalance} ETH`);
          }
          return Number(staleBalance);
        }
      }
    } catch (cacheError) {
      warnLog('Balance', `[fetchERC20BalanceFresh] Failed to get stale cache | wallet=${walletAddress.slice(0, 10)}...`);
    }

    // Return 0 as final fallback
    warnLog('Balance', `[fetchERC20BalanceFresh] No cached balance available, returning 0.00 | wallet=${walletAddress.slice(0, 10)}...`);
    return 0;
  }
}
