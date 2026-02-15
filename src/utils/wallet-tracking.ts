/**
 * Wallet address tracking utilities
 * Maintains in-memory Set of wallet addresses for O(1) self-bid detection
 */

import Wallet from "../models/wallet.model";
import { errorLog } from "./debug-file-logger";

const ourWalletAddresses = new Set<string>();

/**
 * Load all wallet addresses from database into memory.
 * Called on startup after DB connection.
 */
export async function loadOurWalletAddresses(): Promise<void> {
  try {
    const wallets = await Wallet.find({}, { address: 1 });
    ourWalletAddresses.clear();
    wallets.forEach(w => {
      if (w.address) {
        ourWalletAddresses.add(w.address.toLowerCase());
      }
    });
  } catch (error) {
    errorLog('System', 'Failed to load wallet addresses', error);
  }
}

/**
 * Add a wallet address to the tracking set.
 * Call when a new wallet is created via API.
 */
export function addOurWalletAddress(address: string): void {
  if (address) {
    ourWalletAddresses.add(address.toLowerCase());
  }
}

/**
 * Remove a wallet address from the tracking set.
 * Call when a wallet is deleted via API.
 */
export function removeOurWalletAddress(address: string): void {
  if (address) {
    ourWalletAddresses.delete(address.toLowerCase());
  }
}

/**
 * Check if an address belongs to one of our wallets.
 * O(1) lookup for fast counterbid filtering.
 */
export function isOurWallet(address: string): boolean {
  if (!address) return false;
  return ourWalletAddresses.has(address.toLowerCase());
}
