import { ethers } from "ethers";
import { DistributedLockManager } from "./lock";
import redisClient from "./redis";
import { RED, RESET } from "..";

const redis = redisClient.getClient()

export async function fetchERC20Balance(walletAddress: string, tokenAddress: string): Promise<number> {
  const CACHE_EXPIRY_SECONDS = 30;
  const cacheKey = `{erc20_balance:${walletAddress}}:${tokenAddress}`;

  const cachedBalance = await redis.get(cacheKey);
  if (cachedBalance) {
    return Number(cachedBalance);
  }

  const lockManager = new DistributedLockManager({
    lockPrefix: '{erc20_balance}:lock:',
    defaultTTLSeconds: CACHE_EXPIRY_SECONDS
  });

  const lockKey = `{erc20_${walletAddress}}_${tokenAddress}`;

  const result = await lockManager.withLock(
    lockKey,
    async () => {
      const cachedBalanceAfterLock = await redis.get(cacheKey);
      if (cachedBalanceAfterLock) {
        return Number(cachedBalanceAfterLock);
      }

      try {
        const provider = new ethers.providers.JsonRpcProvider('https://ethereum.publicnode.com');

        const tokenContract = new ethers.Contract(
          tokenAddress,
          ['function balanceOf(address) view returns (uint256)'],
          provider
        );
        const maxRetries = 3;
        let lastError;

        for (let attempt = 0; attempt <= maxRetries; attempt++) {
          try {
            const balance = await tokenContract.balanceOf(walletAddress);
            const formattedBalance = Number(ethers.utils.formatEther(balance));
            await redis.set(cacheKey, formattedBalance.toString(), 'EX', CACHE_EXPIRY_SECONDS);

            return formattedBalance;
          } catch (error) {
            lastError = error;
            if (attempt < maxRetries) {
              const backoffTime = Math.pow(2, attempt) * 1000;
              await new Promise(resolve => setTimeout(resolve, backoffTime));
            }
          }
        }

        throw lastError;
      } catch (error) {
        console.error(RED + `Error fetching ERC20 balance for token ${tokenAddress}:`, error + RESET);
        throw error;
      }
    }
  );

  return result || 0;
}
