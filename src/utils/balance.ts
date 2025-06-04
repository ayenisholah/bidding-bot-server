import { ethers } from "ethers";
import { Cluster } from 'ioredis';
import { DistributedLockManager } from './lock';
import RedisClient from './redis';
import { Interface } from "ethers/lib/utils";
import { axiosInstance, limiter } from "../init";
import { RED, RESET } from "..";
// import { BASE_URL } from "../constants";
const BASE_URL = "https://nfttools.pro"

// Constants
const CACHE_EXPIRY_SECONDS = 30;
const BLUR_POOL_ADDRESS = "0x0000000000A39bb272e79075ade125fd351887Ac";
const API_KEY = "a4eae399-f135-4627-829a-18435bb631ae";

// ERC20 ABI for balance checking
const ERC20_ABI = [
  "function balanceOf(address owner) view returns (uint256)",
  "function decimals() view returns (uint8)",
  "function symbol() view returns (string)"
];

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
    } catch (error: any) {
      console.error('Redis Cluster cache error:', error?.response?.data);
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
    } catch (error: any) {
      console.error('Redis Cluster cache set error:', error?.response?.data);
    }
  }

  async getERC20Balance(address: string, tokenAddress: string): Promise<number> {
    const cacheKey = `erc20:${tokenAddress}:${address}`;
    let cachedBalance = await this.getCachedBalance(cacheKey);

    const result = await this.lockManager.withLock(
      `erc20_${tokenAddress}_${address}`,
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
          // Get decimals first
          const decimalsData = new Interface(ERC20_ABI).encodeFunctionData("decimals", []);
          const decimalsRequest = {
            jsonrpc: "2.0",
            method: "eth_call",
            params: [
              {
                to: tokenAddress,
                data: decimalsData,
              },
              "latest",
            ],
            id: 1,
          };

          const decimalsResponse = await limiter.schedule(() =>
            axiosInstance.post(BASE_URL, decimalsRequest, { headers })
          );
          const decimals = parseInt(decimalsResponse.data.result, 16);

          // Get balance
          const balanceData = new Interface(ERC20_ABI).encodeFunctionData("balanceOf", [address]);
          const balanceRequest = {
            jsonrpc: "2.0",
            method: "eth_call",
            params: [
              {
                to: tokenAddress,
                data: balanceData,
              },
              "latest",
            ],
            id: 2,
          };

          const response = await limiter.schedule(() =>
            axiosInstance.post(BASE_URL, balanceRequest, { headers })
          );
          const balance = response.data.result;

          // Format balance according to token decimals
          const formattedBalance = Number(ethers.utils.formatUnits(balance, decimals));
          await this.setCachedBalance(cacheKey, formattedBalance);
          return formattedBalance;

        } catch (error: any) {
          console.error(RED + "Error fetching ERC20 balance:", error?.response?.data + RESET);
          if (cachedBalance !== null) {
            return cachedBalance;
          }
          throw error;
        }
      }
    );

    if (result !== undefined && result !== null) {
      return result;
    }

    if (cachedBalance !== null) {
      return cachedBalance;
    }

    return 0;
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
      } catch (error: any) {
        if (attempt === maxRetries) {
          console.error(RED + "Error fetching BETH balance after retries:", error?.response?.data + RESET);
          if (cachedBalance !== null) {
            return cachedBalance;
          }
          throw error;
        }
        const backoffTime = Math.pow(2, attempt) * 1000; // Exponential backoff
        await new Promise(resolve => setTimeout(resolve, backoffTime));
      }
    }
    }
    );

    if (result !== undefined && result !== null) {
      return result;
    }

    if (cachedBalance !== null) {
      return cachedBalance;
    }

    return 0;
  }
}

// Usage example
export function createBalanceChecker(deps: Dependencies): BalanceChecker {
  return new BalanceChecker(deps);
}
