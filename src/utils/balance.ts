import {  formatEther, JsonRpcProvider, Contract } from "ethers";
import { DistributedLockManager } from "./lock";
import redisClient from "./redis";
import { RED, RESET } from "..";
import { logger, ErrorCategory } from "./logger";

const getRedis = () => redisClient.getClient()

export async function fetchERC20Balance(walletAddress: string, tokenAddress: string): Promise<number> {
  const CACHE_EXPIRY_SECONDS = 60;
  const STALE_CACHE_EXPIRY_SECONDS = 3600; // Keep stale cache for 1 hour as fallback
  const cacheKey = `{erc20_balance:${walletAddress}}:${tokenAddress}`;
  const staleCacheKey = `{erc20_balance_stale:${walletAddress}}:${tokenAddress}`;

  // Try to get fresh cached balance first
  try {
    const redis = getRedis();
    if (redis) {
      const cachedBalance = await redis.get(cacheKey);
      if (cachedBalance) {
        return Number(cachedBalance);
      }
    }
  } catch (error) {
    console.warn('Redis not available for cache lookup:', error);
  }

  const lockManager = new DistributedLockManager({
    lockPrefix: '{erc20_balance}:lock:',
    defaultTTLSeconds: CACHE_EXPIRY_SECONDS
  });

  const lockKey = `{erc20_${walletAddress}}_${tokenAddress}`;

  try {
    const result = await lockManager.withLock(
      lockKey,
      async () => {
        try {
          const redis = getRedis();
          if (redis) {
            const cachedBalanceAfterLock = await redis.get(cacheKey);
            if (cachedBalanceAfterLock) {
              return Number(cachedBalanceAfterLock);
            }
          }
        } catch (error) {
          console.warn('Redis not available for cache lookup after lock:', error);
        }

        try {
          // List of RPC providers to try
          const rpcUrls = [
            'https://ethereum.publicnode.com',
            'https://eth.llamarpc.com',
            'https://rpc.ankr.com/eth',
            'https://cloudflare-eth.com'
          ];
          
          let provider: JsonRpcProvider | null = null;
          let providerError: any = null;
          
          // Try each RPC provider until one works
          for (const rpcUrl of rpcUrls) {
            try {
              provider = new JsonRpcProvider(rpcUrl, undefined, {
                staticNetwork: true,
                batchMaxCount: 1
              });
              
              // Set a timeout for network detection
              const networkDetectionTimeout = new Promise((_, reject) => 
                setTimeout(() => reject(new Error('Network detection timeout')), 5000)
              );
              
              // Wait for the provider to be ready with timeout
              await Promise.race([
                provider._detectNetwork(),
                networkDetectionTimeout
              ]);
              
              // Verify connection
              await provider.getBlockNumber();
              break;
            } catch (err) {
              providerError = err;
              console.warn(`Failed to connect to ${rpcUrl}, trying next...`);
            }
          }
          
          if (!provider) {
            await logger.logNetworkError('all_rpc_providers', `All RPC providers failed. Last error: ${providerError?.message}`, {
              walletAddress,
              tokenAddress,
              lastError: providerError,
              errorMessage: providerError?.message,
              operation: 'fetch_erc20_balance'
            });
            throw new Error(`All RPC providers failed. Last error: ${providerError?.message}`);
          }

          const tokenContract = new Contract(
            tokenAddress,
            ['function balanceOf(address) view returns (uint256)'],
            provider
          );
          const maxRetries = 3;
          let lastError;

          for (let attempt = 0; attempt <= maxRetries; attempt++) {
            try {
              const balance = await tokenContract.balanceOf(walletAddress);
              const formattedBalance = Number(formatEther(balance));
              
              // Store in both fresh and stale cache
              try {
                const redis = getRedis();
                if (redis) {
                  await redis.set(cacheKey, formattedBalance.toString(), 'EX', CACHE_EXPIRY_SECONDS);
                  await redis.set(staleCacheKey, formattedBalance.toString(), 'EX', STALE_CACHE_EXPIRY_SECONDS);
                }
              } catch (error) {
                console.warn('Failed to cache balance:', error);
              }

              return formattedBalance;
            } catch (error: any) {
              lastError = error;
              
              // Check if it's a network error
              if (error.code === 'NETWORK_ERROR' || error.message?.includes('could not detect network')) {
                console.warn(`Network error for ${walletAddress}/${tokenAddress}, attempt ${attempt + 1}/${maxRetries + 1}`);
              }
              
              if (attempt < maxRetries) {
                const backoffTime = Math.pow(2, attempt) * 1000;
                await new Promise(resolve => setTimeout(resolve, backoffTime));
              }
            }
          }

          throw lastError;
        } catch (error) {
          await logger.logBlockchainError(`Error fetching ERC20 balance for token ${tokenAddress}`, {
            walletAddress,
            tokenAddress,
            operation: 'fetch_erc20_balance',
            error: error as Error,
            errorMessage: error instanceof Error ? error.message : String(error)
          });
          console.error(RED + `Error fetching ERC20 balance for token ${tokenAddress}:` + RESET, error);
          throw error;
        }
      }
    );

    return result || 0;
  } catch (error: any) {
    // If we get a network error or lock error, try to use stale cache
    console.warn(`Failed to fetch fresh balance for ${walletAddress}/${tokenAddress}, checking stale cache...`, error.message || error);
    
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
      console.log(`Using stale cached balance for ${walletAddress}/${tokenAddress}: ${staleBalance}`);
      return Number(staleBalance);
    }
    
    // If no stale cache available, log error and return 0
    await logger.logWarning(ErrorCategory.BLOCKCHAIN, `No cached balance available for ${walletAddress}/${tokenAddress}`, {
      walletAddress,
      tokenAddress,
      operation: 'fetch_erc20_balance_fallback'
    });
    console.error(RED + `No cached balance available for ${walletAddress}/${tokenAddress}` + RESET);
    return 0;
  }
}
