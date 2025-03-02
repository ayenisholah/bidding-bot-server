import { ethers } from "ethers";
import { Cluster } from 'ioredis';
import { DistributedLockManager } from './lock';
import RedisClient from './redis';
import { formatEther, Interface } from "ethers/lib/utils";
import { axiosInstance, limiter } from "../init";

// Constants
const CACHE_EXPIRY_SECONDS = 60;
const BLUR_POOL_ADDRESS = "0x0000000000A39bb272e79075ade125fd351887Ac";
const WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
const API_ENDPOINT = "https://nfttools.pro";
const API_KEY = "a4eae399-f135-4627-829a-18435bb631ae";

// Environment configuration
const config = {
  API_KEY: API_KEY,
  ALCHEMY_KEY: process.env.ALCHEMY_KEY,
  OPENSEA_API_URL: 'https://nfttools.pro/opensea/__api/graphql/',
};

// Type Definitions
type BalanceType = 'weth' | 'beth';

interface Dependencies {
  provider: ethers.providers.Provider;
}

class BalanceChecker {
  private deps: Dependencies;
  private lockManager: DistributedLockManager;
  private redis: Cluster;

  constructor(deps: Dependencies) {
    this.deps = deps;
    this.redis = RedisClient.getClient();
    this.lockManager = new DistributedLockManager({
      lockPrefix: '{balance_lock}:',
      defaultTTLSeconds: CACHE_EXPIRY_SECONDS
    });
  }

  private async getCachedBalance(cacheKey: string): Promise<number | null> {
    try {
      const cachedBalance = await this.redis.get(cacheKey);
      return cachedBalance ? Number(cachedBalance) : null;
    } catch (error) {
      console.error('Redis Cluster cache error:', error);
      return null;
    }
  }

  private async setCachedBalance(cacheKey: string, balance: number): Promise<void> {
    try {
      await this.redis.set(
        cacheKey,
        balance.toString(),
        'EX',
        CACHE_EXPIRY_SECONDS
      );
    } catch (error) {
      console.error('Redis Cluster cache set error:', error);
    }
  }

  async getWethBalance(address: string): Promise<number> {
    const cacheKey = `weth_balance:${address}`;
    let cachedBalance = await this.getCachedBalance(cacheKey);

    const result = await this.lockManager.withLock(
      `weth_${address}`,
      async () => {
        const cachedBalanceAfterLock = await this.getCachedBalance(cacheKey);
        if (cachedBalanceAfterLock !== null) {
          return cachedBalanceAfterLock;
        }

        try {
          // Common headers for request
          const headers = {
            url: "https://ethereum.publicnode.com",
            "x-nft-api-key": API_KEY,
            "Content-Type": "application/json",
          };

          // WETH Balance Request
          const wethData = new Interface([
            "function balanceOf(address owner) view returns (uint256)",
          ]).encodeFunctionData("balanceOf", [address]);

          const wethBalanceRequest = {
            jsonrpc: "2.0",
            method: "eth_call",
            params: [
              {
                to: WETH_ADDRESS,
                data: wethData,
              },
              "latest",
            ],
            id: 2,
          };

          const response = await limiter.schedule(() => axiosInstance.post(API_ENDPOINT, wethBalanceRequest, { headers }));
          const balance = response.data.result;
          const formattedBalance = Number(formatEther(balance));

          await this.setCachedBalance(cacheKey, formattedBalance);
          return formattedBalance;

        } catch (error) {
          console.error("Error fetching WETH balance:", error);
          return cachedBalance ?? 0;
        }
      }
    );

    return result ?? (cachedBalance ?? 0);
  }

  async getBethBalance(address: string): Promise<number> {
    const cacheKey = `beth_balance:${address}`;
    let cachedBalance = await this.getCachedBalance(cacheKey);

    const result = await this.lockManager.withLock(
      `beth_${address}`,
      async () => {
        const cachedBalanceAfterLock = await this.getCachedBalance(cacheKey);
        if (cachedBalanceAfterLock !== null) {
          return cachedBalanceAfterLock;
        }

        const wethContract = new ethers.Contract(
          BLUR_POOL_ADDRESS,
          ['function balanceOf(address) view returns (uint256)'],
          this.deps.provider
        );

        const maxRetries = 3;
        for (let attempt = 0; attempt <= maxRetries; attempt++) {
          try {
            const balance = await wethContract.balanceOf(address);
            const formattedBalance = Number(ethers.utils.formatEther(balance));

            await this.setCachedBalance(cacheKey, formattedBalance);
            return formattedBalance;
          } catch (error) {
            if (attempt === maxRetries) {
              console.error("Error fetching BETH balance after retries:", error);
              return cachedBalance ?? 0;
            }
            const backoffTime = Math.pow(2, attempt) * 1000; // Exponential backoff
            await new Promise(resolve => setTimeout(resolve, backoffTime));
          }
        }
      }
    );

    return result ?? (cachedBalance ?? 0);
  }
}

// Usage example
export function createBalanceChecker(deps: Dependencies): BalanceChecker {
  return new BalanceChecker(deps);
}
