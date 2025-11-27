import { DistributedLockManager } from "./lock";
import redisClient from "./redis";
import { RED, RESET, API_KEY } from "..";
import { logger, ErrorCategory } from "./logger";
import { limiter, axiosInstance } from "../init";
import { BASE_URL } from "../constants";

const getRedis = () => redisClient.getClient()

const WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".toLowerCase();

// Debug logging controlled by environment variable
const DEBUG_WETH_BALANCE = process.env.DEBUG_WETH_BALANCE === 'true';

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

  if (DEBUG_WETH_BALANCE) {
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

  if (DEBUG_WETH_BALANCE) {
    const elapsed = Date.now() - startTime;
    console.log(`[WETH Balance] GraphQL fetch complete: wallet=${truncateAddress(walletAddress)}, balance=${balance} (${formatWeiToEth(balance)} ETH), elapsed=${elapsed}ms`);
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
    console.warn('[Balance] Failed to invalidate cache:', error);
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
        if (DEBUG_WETH_BALANCE) {
          console.log(`[WETH Balance] Cache hit (fresh): wallet=${truncateAddress(walletAddress)}, balance=${cachedBalance} (${formatWeiToEth(Number(cachedBalance))} ETH)`);
        }
        return Number(cachedBalance);
      }
    }
  } catch (error) {
    console.warn('Redis not available for cache lookup:', error);
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
              if (DEBUG_WETH_BALANCE) {
                console.log(`[WETH Balance] Cache hit (after lock): wallet=${truncateAddress(walletAddress)}, balance=${cachedBalanceAfterLock} (${formatWeiToEth(Number(cachedBalanceAfterLock))} ETH)`);
              }
              return Number(cachedBalanceAfterLock);
            }
          }
        } catch (error) {
          console.warn('Redis not available for cache lookup after lock:', error);
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
            console.warn('Failed to cache balance:', error);
          }

          if (DEBUG_WETH_BALANCE) {
            console.log(`[WETH Balance] Fresh fetch success: wallet=${truncateAddress(walletAddress)}, balance=${balance} (${formatWeiToEth(balance)} ETH)`);
          }

          return balance;
        } catch (error) {
          await logger.logBlockchainError(`Error fetching WETH balance via GraphQL`, {
            walletAddress,
            operation: 'fetch_weth_balance_graphql',
            error: error as Error,
            errorMessage: error instanceof Error ? error.message : String(error)
          });
          console.error(RED + `Error fetching WETH balance via GraphQL:` + RESET, error);
          throw error;
        }
      }
    );

    return result || 0;
  } catch (error: any) {
    // If we get a network error or lock error, try to use stale cache
    console.warn(`Failed to fetch fresh WETH balance for ${walletAddress}, checking stale cache...`, error.message || error);

    let staleBalance = null;
    try {
      const redis = getRedis();
      if (redis) {
        staleBalance = await redis.get(staleCacheKey);
      }
    } catch (error) {
      console.warn('Failed to get stale cache:', error);
    }
    if (staleBalance) {
      if (DEBUG_WETH_BALANCE) {
        console.log(`[WETH Balance] Stale cache fallback: wallet=${truncateAddress(walletAddress)}, balance=${staleBalance} (${formatWeiToEth(Number(staleBalance))} ETH)`);
      }
      return Number(staleBalance);
    }

    // If no stale cache available, log error and return 0
    await logger.logWarning(ErrorCategory.BLOCKCHAIN, `No cached WETH balance available for ${walletAddress}`, {
      walletAddress,
      operation: 'fetch_weth_balance_fallback'
    });
    console.error(RED + `No cached WETH balance available for ${walletAddress}` + RESET);
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
        if (DEBUG_WETH_BALANCE) {
          console.log(`[WETH Balance Fresh] Cache hit: wallet=${truncateAddress(walletAddress)}, balance=${cachedBalance} (${formatWeiToEth(Number(cachedBalance))} ETH)`);
        }
        return Number(cachedBalance);
      }
    }
  } catch (error) {
    console.warn('[fetchERC20BalanceFresh] Redis error when checking cache:', error);
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
              console.warn('Failed to cache balance:', error);
            }

            if (DEBUG_WETH_BALANCE) {
              const elapsed = Date.now() - startTime;
              console.log(`[WETH Balance Fresh] Fresh fetch success: wallet=${truncateAddress(walletAddress)}, balance=${balance} (${formatWeiToEth(balance)} ETH), elapsed=${elapsed}ms`);
            }

            return balance;
          } catch (error) {
            console.error(RED + `Error fetching fresh WETH balance via GraphQL:` + RESET, error);
            throw error;
          }
        }
      );

      return result || 0;
    } catch (error: any) {
      console.error(RED + `Lock/fetch error for ${walletAddress}: ${error.message}` + RESET);
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
    console.warn(`[fetchERC20BalanceFresh] Failed after ${elapsed}ms: ${errorMsg}, checking stale cache...`);

    // Try stale cache
    try {
      const redis = getRedis();
      if (redis) {
        const staleBalance = await redis.get(staleCacheKey);
        if (staleBalance) {
          if (DEBUG_WETH_BALANCE) {
            console.log(`[WETH Balance Fresh] Stale cache fallback: wallet=${truncateAddress(walletAddress)}, balance=${staleBalance} (${formatWeiToEth(Number(staleBalance))} ETH)`);
          }
          return Number(staleBalance);
        }
      }
    } catch (cacheError) {
      console.warn('[fetchERC20BalanceFresh] Failed to get stale cache:', cacheError);
    }

    // Return 0 as final fallback
    console.warn(`[fetchERC20BalanceFresh] No cached balance available, returning 0.00`);
    return 0;
  }
}
