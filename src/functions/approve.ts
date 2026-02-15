import { Contract, Wallet } from "ethers";
import { errorLog, warnLog } from "../utils/debug-file-logger";
import { LRUCache } from 'lru-cache';

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

// LRU cache for NFT approvals (24 hour TTL, 10K max entries)
const nftApprovalLRU = new LRUCache<string, boolean>({
  max: 10000,
  ttl: 24 * 60 * 60 * 1000, // 24 hours
  allowStale: false,
  updateAgeOnGet: false
});

export async function approveNFT(
  nftAddress: string,
  wallet: Wallet,
  marketContractAddress: string
): Promise<void> {
  const walletAddress = wallet.address.toLowerCase();

  // Create cache key: walletAddress:nftAddress:marketContractAddress
  const cacheKey = `${walletAddress}:${nftAddress.toLowerCase()}:${marketContractAddress.toLowerCase()}`;

  try {
    // Check LRU cache first
    const cachedApproval = nftApprovalLRU.get(cacheKey);
    if (cachedApproval === true) {
      // console.log(`NFT approval found in cache for ${nftAddress}`);
      return;
    }

    // If not in cache, check blockchain
    const nftContract = new Contract(nftAddress, approveAbi, wallet);
    const approved: boolean = await nftContract.isApprovedForAll(
      wallet.address,
      marketContractAddress
    );

    if (approved) {
      // Cache the approval status
      nftApprovalLRU.set(cacheKey, true);
      // console.log(`Cached NFT approval for ${nftAddress}`);
    } else {
      // Need to approve
      const tx = await nftContract.setApprovalForAll(
        marketContractAddress,
        true
      );
      await tx.wait();
      // console.log('NFT approved successfully');

      // Cache the new approval
      nftApprovalLRU.set(cacheKey, true);
      // console.log(`Cached new NFT approval for ${nftAddress}`);
    }
  } catch (err: any) {
    // If approval fails, don't cache anything
    errorLog('Blockchain', `[approve.ts] Error approving NFT for marketplace | nftAddress=${nftAddress} | wallet=${wallet.address} | market=${marketContractAddress}`, err);
    // console.log(err?.reason ?? err?.message);
  }
};

/**
 * Export cache for monitoring (used by memory-snapshot.service.ts)
 */
export function getNftApprovalCache(): LRUCache<string, boolean> {
  return nftApprovalLRU;
}