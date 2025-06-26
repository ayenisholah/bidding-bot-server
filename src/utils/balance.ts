import {  formatEther, JsonRpcProvider, Contract } from "ethers";
import { DistributedLockManager } from "./lock";
import redisClient from "./redis";
import { RED, RESET } from "..";

const redis = redisClient.getClient()

export async function fetchERC20Balance(walletAddress: string, tokenAddress: string): Promise<number> {
  const CACHE_EXPIRY_SECONDS = 60;
  const STALE_CACHE_EXPIRY_SECONDS = 3600; // Keep stale cache for 1 hour as fallback
  const cacheKey = `{erc20_balance:${walletAddress}}:${tokenAddress}`;
  const staleCacheKey = `{erc20_balance_stale:${walletAddress}}:${tokenAddress}`;

  // Try to get fresh cached balance first
  const cachedBalance = await redis.get(cacheKey);
  if (cachedBalance) {
    return Number(cachedBalance);
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
        const cachedBalanceAfterLock = await redis.get(cacheKey);
        if (cachedBalanceAfterLock) {
          return Number(cachedBalanceAfterLock);
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
              await redis.set(cacheKey, formattedBalance.toString(), 'EX', CACHE_EXPIRY_SECONDS);
              await redis.set(staleCacheKey, formattedBalance.toString(), 'EX', STALE_CACHE_EXPIRY_SECONDS);

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
          console.error(RED + `Error fetching ERC20 balance for token ${tokenAddress}:` + RESET, error);
          throw error;
        }
      }
    );

    return result || 0;
  } catch (error: any) {
    // If we get a network error or lock error, try to use stale cache
    console.warn(`Failed to fetch fresh balance for ${walletAddress}/${tokenAddress}, checking stale cache...`, error.message || error);
    
    const staleBalance = await redis.get(staleCacheKey);
    if (staleBalance) {
      console.log(`Using stale cached balance for ${walletAddress}/${tokenAddress}: ${staleBalance}`);
      return Number(staleBalance);
    }
    
    // If no stale cache available, log error and return 0
    console.error(RED + `No cached balance available for ${walletAddress}/${tokenAddress}` + RESET);
    return 0;
  }
}
