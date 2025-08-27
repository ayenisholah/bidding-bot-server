import { Contract, Wallet } from "ethers";
import { logger } from "../utils/logger";
import redisClient from "../utils/redis";

const approveAbi = [
  {
    inputs: [
      {
        internalType: 'address',
        name: 'account',
        type: 'address'
      },
      {
        internalType: 'address',
        name: 'operator',
        type: 'address'
      }
    ],
    name: 'isApprovedForAll',
    outputs: [
      {
        internalType: 'bool',
        name: '',
        type: 'bool'
      }
    ],
    stateMutability: 'view',
    type: 'function'
  },
  {
    inputs: [
      {
        internalType: 'address',
        name: 'operator',
        type: 'address'
      },
      {
        internalType: 'bool',
        name: 'approved',
        type: 'bool'
      }
    ],
    name: 'setApprovalForAll',
    outputs: [],
    stateMutability: 'nonpayable',
    type: 'function'
  }
];

// Cache TTL: 24 hours for NFT approvals (they rarely change)
const NFT_APPROVAL_CACHE_TTL = 24 * 60 * 60; // 24 hours in seconds

export async function approveNFTCached(
  nftAddress: string,
  wallet: Wallet,
  marketContractAddress: string
): Promise<void> {
  const redis = redisClient.getClient();
  const walletAddress = wallet.address.toLowerCase();
  
  // Create cache key: {nft_approval}:walletAddress:nftAddress:marketContractAddress
  const cacheKey = `nft_approval:${walletAddress}:${nftAddress.toLowerCase()}:${marketContractAddress.toLowerCase()}`;
  
  try {
    // Check Redis cache first
    if (redis && redisClient.isReady()) {
      try {
        const cachedApproval = await redis.get(cacheKey);
        if (cachedApproval === 'true') {
          console.log(`NFT approval found in cache for ${nftAddress}`);
          return;
        }
      } catch (cacheError) {
        console.warn('Failed to check NFT approval cache:', cacheError);
        // Continue without cache if Redis fails
      }
    }

    // If not in cache, check blockchain
    const nftContract = new Contract(nftAddress, approveAbi, wallet);
    const approved: boolean = await nftContract.isApprovedForAll(
      walletAddress,
      marketContractAddress
    );
    
    if (approved) {
      // Cache the approval status
      if (redis && redisClient.isReady()) {
        try {
          await redis.set(cacheKey, 'true', 'EX', NFT_APPROVAL_CACHE_TTL);
          console.log(`Cached NFT approval for ${nftAddress}`);
        } catch (cacheError) {
          console.warn('Failed to cache NFT approval:', cacheError);
        }
      }
    } else {
      // Need to approve
      const tx = await nftContract.setApprovalForAll(
        marketContractAddress,
        true
      );
      await tx.wait();
      console.log('NFT approved successfully');
      
      // Cache the new approval
      if (redis && redisClient.isReady()) {
        try {
          await redis.set(cacheKey, 'true', 'EX', NFT_APPROVAL_CACHE_TTL);
          console.log(`Cached new NFT approval for ${nftAddress}`);
        } catch (cacheError) {
          console.warn('Failed to cache new NFT approval:', cacheError);
        }
      }
    }
  } catch (err: any) {
    // If approval fails, don't cache anything
    await logger.logBlockchainError('Error approving NFT for marketplace', {
      nftAddress,
      walletAddress: wallet.address,
      marketContractAddress,
      operation: 'approve_nft',
      error: err,
      errorCode: err?.code,
      errorMessage: err?.reason ?? err?.message
    });
    console.log(err?.reason ?? err?.message);
    throw err; // Re-throw to maintain existing behavior
  }
}

// Helper function to clear cache when needed (e.g., after revoking approval)
export async function clearNFTApprovalCache(
  walletAddress: string,
  nftAddress: string,
  marketContractAddress: string
): Promise<void> {
  const redis = redisClient.getClient();
  if (!redis || !redisClient.isReady()) return;
  
  const cacheKey = `nft_approval:${walletAddress.toLowerCase()}:${nftAddress.toLowerCase()}:${marketContractAddress.toLowerCase()}`;
  
  try {
    await redis.del(cacheKey);
    console.log(`Cleared NFT approval cache for ${nftAddress}`);
  } catch (error) {
    console.warn('Failed to clear NFT approval cache:', error);
  }
}

// Bulk check function for multiple NFTs (useful for batch operations)
export async function checkMultipleNFTApprovals(
  wallet: Wallet,
  nftAddresses: string[],
  marketContractAddress: string
): Promise<Map<string, boolean>> {
  const redis = redisClient.getClient();
  const walletAddress = wallet.address.toLowerCase();
  const results = new Map<string, boolean>();
  const uncachedNFTs: string[] = [];
  
  // First, check cache for all NFTs
  if (redis && redisClient.isReady()) {
    for (const nftAddress of nftAddresses) {
      const cacheKey = `nft_approval:${walletAddress}:${nftAddress.toLowerCase()}:${marketContractAddress.toLowerCase()}`;
      try {
        const cached = await redis.get(cacheKey);
        if (cached === 'true') {
          results.set(nftAddress, true);
        } else {
          uncachedNFTs.push(nftAddress);
        }
      } catch (error) {
        uncachedNFTs.push(nftAddress);
      }
    }
  } else {
    uncachedNFTs.push(...nftAddresses);
  }
  
  // Check blockchain for uncached NFTs
  for (const nftAddress of uncachedNFTs) {
    try {
      const nftContract = new Contract(nftAddress, approveAbi, wallet.provider);
      const approved = await nftContract.isApprovedForAll(walletAddress, marketContractAddress);
      results.set(nftAddress, approved);
      
      // Cache the result
      if (approved && redis && redisClient.isReady()) {
        const cacheKey = `nft_approval:${walletAddress}:${nftAddress.toLowerCase()}:${marketContractAddress.toLowerCase()}`;
        try {
          await redis.set(cacheKey, 'true', 'EX', NFT_APPROVAL_CACHE_TTL);
        } catch (error) {
          console.warn('Failed to cache NFT approval:', error);
        }
      }
    } catch (error) {
      console.error(`Failed to check approval for ${nftAddress}:`, error);
      results.set(nftAddress, false);
    }
  }
  
  return results;
}