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

export async function approveNFT(
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
          // console.log(`NFT approval found in cache for ${nftAddress}`);
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
      wallet.address,
      marketContractAddress
    );
    
    if (approved) {
      // Cache the approval status
      if (redis && redisClient.isReady()) {
        try {
          await redis.set(cacheKey, 'true', 'EX', NFT_APPROVAL_CACHE_TTL);
          // console.log(`Cached NFT approval for ${nftAddress}`);
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
      // console.log('NFT approved successfully');
      
      // Cache the new approval
      if (redis && redisClient.isReady()) {
        try {
          await redis.set(cacheKey, 'true', 'EX', NFT_APPROVAL_CACHE_TTL);
          // console.log(`Cached new NFT approval for ${nftAddress}`);
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
    // console.log(err?.reason ?? err?.message);
  }
};