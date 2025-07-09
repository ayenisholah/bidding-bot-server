import { Contract, ethers, Wallet as Web3Wallet, MaxUint256 } from "ethers";
import { PrismaClient } from '@prisma/client';
import redisClient from "../utils/redis";
import { ERC20_MIN_ABI } from "../constants";
import { WETH } from "../utils/currencies";

const prisma = new PrismaClient();

// Cache TTL: 7 days for marketplace approvals (they rarely change)
const MARKETPLACE_APPROVAL_CACHE_TTL = 7 * 24 * 60 * 60; // 7 days in seconds

// Retry configuration
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

async function withRetry<T>(fn: () => Promise<T>, retries = MAX_RETRIES): Promise<T> {
  try {
    return await fn();
  } catch (error: any) {
    if (retries > 0) {
      console.log(`Retrying operation, ${retries} attempts remaining...`);
      await delay(RETRY_DELAY);
      return withRetry(fn, retries - 1);
    }
    throw error;
  }
}

// Track ongoing approvals to prevent duplicate transactions
const approvalLocks = new Map<string, Promise<boolean>>();

/**
 * Approves a marketplace contract to spend tokens with Redis caching
 * @param marketplace The marketplace contract address to approve
 * @param privateKey The private key of the wallet
 * @param currency The currency to approve (default: WETH)
 * @param alchemyApiKey The Alchemy API key
 * @returns Promise<boolean> Whether the approval was successful
 */
export async function approveMarketplaceWithCache(
  marketplace: string,
  privateKey: string,
  currency: string = WETH,
  alchemyApiKey: string
): Promise<boolean> {
  const redis = redisClient.getClient();
  
  try {
    // Set up provider and wallet
    const provider = await withRetry(async () => {
      const p = new ethers.AlchemyProvider('mainnet', alchemyApiKey);
      await p.getNetwork(); // Test connection
      return p;
    });

    const wallet = new Web3Wallet(privateKey, provider);
    const address = wallet.address.toLowerCase();
    const marketplaceLower = marketplace.toLowerCase();
    
    // Create cache keys
    const redisCacheKey = `{marketplace_approval}:${address}:${marketplaceLower}:${currency.toLowerCase()}`;
    const approvalLockKey = `{${address}}:${marketplaceLower}:${currency.toLowerCase()}`;
    
    // Check Redis cache first
    if (redis && redisClient.isReady()) {
      try {
        const cachedApproval = await redis.get(redisCacheKey);
        if (cachedApproval === 'true') {
          console.log(`Marketplace approval found in Redis cache for ${marketplace}`);
          return true;
        }
      } catch (cacheError) {
        console.warn('Failed to check marketplace approval cache:', cacheError);
      }
    }
    
    // Check database for existing approval
    const existingApproval = await prisma.wallet.findUnique({ 
      where: { address, currency } 
    });
    
    // Return early if marketplace is already approved in DB
    const SEAPORT = '0x1e0049783f008a0085193e00003d00cd54003c71';
    const MAGICEDEN_MARKETPLACE = "0x9A1D00bEd7CD04BCDA516d721A596eb22Aac6834";
    
    if (marketplaceLower === SEAPORT.toLowerCase() && existingApproval?.openseaApproval) {
      // Cache in Redis
      if (redis && redisClient.isReady()) {
        try {
          await redis.set(redisCacheKey, 'true', 'EX', MARKETPLACE_APPROVAL_CACHE_TTL);
        } catch (error) {
          console.warn('Failed to cache marketplace approval:', error);
        }
      }
      return true;
    }
    
    if (marketplaceLower === MAGICEDEN_MARKETPLACE.toLowerCase() && existingApproval?.magicedenApproval) {
      // Cache in Redis
      if (redis && redisClient.isReady()) {
        try {
          await redis.set(redisCacheKey, 'true', 'EX', MARKETPLACE_APPROVAL_CACHE_TTL);
        } catch (error) {
          console.warn('Failed to cache marketplace approval:', error);
        }
      }
      return true;
    }
    
    // If approval is already in progress, return existing promise
    if (approvalLocks.has(approvalLockKey)) {
      return approvalLocks.get(approvalLockKey)!;
    }
    
    // Create new approval process
    const approvalPromise = (async () => {
      try {
        // Create token contract instance
        const erc20Contract = new Contract(currency, ERC20_MIN_ABI, wallet);
        
        // Check current allowance on blockchain
        const allowance = await withRetry(async () => {
          return await erc20Contract.allowance(address, marketplace);
        });
        
        if (allowance >= (MaxUint256 / 2n)) {
          // Already approved on-chain, update database and cache
          const updateData = marketplaceLower === SEAPORT.toLowerCase()
            ? { openseaApproval: true }
            : { magicedenApproval: true };
          
          await prisma.wallet.upsert({
            where: { address },
            update: updateData,
            create: {
              address,
              currency,
              ...updateData
            }
          });
          
          // Cache in Redis
          if (redis && redisClient.isReady()) {
            try {
              await redis.set(redisCacheKey, 'true', 'EX', MARKETPLACE_APPROVAL_CACHE_TTL);
              console.log(`Cached marketplace approval for ${marketplace}`);
            } catch (error) {
              console.warn('Failed to cache marketplace approval:', error);
            }
          }
          
          return true;
        }
        
        // Submit approval transaction
        console.log(`Submitting approval transaction for ${marketplace}...`);
        const tx = await withRetry(async () => {
          return await erc20Contract.approve(marketplace, MaxUint256);
        });
        
        console.log(`Waiting for approval transaction confirmation...`);
        await withRetry(async () => {
          await tx.wait();
        });
        
        console.log(`Marketplace ${marketplace} approved successfully`);
        
        // Update database
        const updateData = marketplaceLower === SEAPORT.toLowerCase()
          ? { openseaApproval: true }
          : { magicedenApproval: true };
        
        await prisma.wallet.upsert({
          where: { address },
          update: updateData,
          create: {
            address,
            currency,
            ...updateData
          }
        });
        
        // Cache in Redis
        if (redis && redisClient.isReady()) {
          try {
            await redis.set(redisCacheKey, 'true', 'EX', MARKETPLACE_APPROVAL_CACHE_TTL);
            console.log(`Cached new marketplace approval for ${marketplace}`);
          } catch (error) {
            console.warn('Failed to cache new marketplace approval:', error);
          }
        }
        
        return true;
      } catch (error: any) {
        console.error(`Approval failed for ${marketplace}:`, {
          error: error.message,
          code: error.code,
          reason: error.reason,
          details: error.error || 'No additional details'
        });
        return false;
      } finally {
        // Clean up lock
        approvalLocks.delete(approvalLockKey);
      }
    })();
    
    // Store promise in locks map
    approvalLocks.set(approvalLockKey, approvalPromise);
    return approvalPromise;
    
  } catch (error: any) {
    console.error('Fatal error in approveMarketplace:', {
      error: error.message,
      code: error.code,
      reason: error.reason,
      details: error.error || 'No additional details'
    });
    return false;
  }
}

/**
 * Clear marketplace approval cache (useful when approval is revoked)
 */
export async function clearMarketplaceApprovalCache(
  walletAddress: string,
  marketplace: string,
  currency: string = WETH
): Promise<void> {
  const redis = redisClient.getClient();
  if (!redis || !redisClient.isReady()) return;
  
  const cacheKey = `{marketplace_approval}:${walletAddress.toLowerCase()}:${marketplace.toLowerCase()}:${currency.toLowerCase()}`;
  
  try {
    await redis.del(cacheKey);
    console.log(`Cleared marketplace approval cache for ${marketplace}`);
  } catch (error) {
    console.warn('Failed to clear marketplace approval cache:', error);
  }
}

/**
 * Bulk check marketplace approvals for multiple wallets
 */
export async function checkMultipleMarketplaceApprovals(
  wallets: { address: string; privateKey: string }[],
  marketplace: string,
  currency: string = WETH,
  alchemyApiKey: string
): Promise<Map<string, boolean>> {
  const redis = redisClient.getClient();
  const results = new Map<string, boolean>();
  const uncachedWallets: typeof wallets = [];
  
  // Check cache first
  if (redis && redisClient.isReady()) {
    for (const wallet of wallets) {
      const address = wallet.address.toLowerCase();
      const cacheKey = `{marketplace_approval}:${address}:${marketplace.toLowerCase()}:${currency.toLowerCase()}`;
      
      try {
        const cached = await redis.get(cacheKey);
        if (cached === 'true') {
          results.set(address, true);
        } else {
          uncachedWallets.push(wallet);
        }
      } catch (error) {
        uncachedWallets.push(wallet);
      }
    }
  } else {
    uncachedWallets.push(...wallets);
  }
  
  // Check database for uncached wallets
  if (uncachedWallets.length > 0) {
    const addresses = uncachedWallets.map(w => w.address.toLowerCase());
    const dbApprovals = await prisma.wallet.findMany({
      where: {
        address: { in: addresses },
        currency
      }
    });
    
    const SEAPORT = '0x1e0049783f008a0085193e00003d00cd54003c71';
    const MAGICEDEN_MARKETPLACE = "0x9A1D00bEd7CD04BCDA516d721A596eb22Aac6834";
    const isSeaport = marketplace.toLowerCase() === SEAPORT.toLowerCase();
    
    for (const approval of dbApprovals) {
      const approved = isSeaport ? approval.openseaApproval : approval.magicedenApproval;
      if (approved) {
        results.set(approval.address, true);
        
        // Cache the result
        if (redis && redisClient.isReady()) {
          const cacheKey = `{marketplace_approval}:${approval.address}:${marketplace.toLowerCase()}:${currency.toLowerCase()}`;
          try {
            await redis.set(cacheKey, 'true', 'EX', MARKETPLACE_APPROVAL_CACHE_TTL);
          } catch (error) {
            console.warn('Failed to cache marketplace approval:', error);
          }
        }
      }
    }
  }
  
  return results;
}