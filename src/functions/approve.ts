import { Contract, Wallet } from "ethers";
import { logger } from "../utils/logger";
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

export async function approveNFT(
  nftAddress: string,
  wallet: Wallet,
  marketContractAddress: string
): Promise<void> {
  const nftContract = new Contract(nftAddress, approveAbi, wallet);
  try {
    const approved: boolean = await nftContract.isApprovedForAll(
      wallet.address,
      marketContractAddress
    );
    if (!approved) {
      const tx = await nftContract.setApprovalForAll(
        marketContractAddress,
        true
      );
      await tx.wait();
      console.log('NFT approved successfully');
    }
  } catch (err: any) {
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
  }
};