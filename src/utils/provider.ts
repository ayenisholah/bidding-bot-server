/**
 * Shared Alchemy JsonRpcProvider instances
 *
 * Provides centralized RPC provider management with fallback capability.
 * All blockchain operations should use these shared providers to reduce
 * memory usage and share connection pooling.
 */
import { JsonRpcProvider } from 'ethers';
import { config } from 'dotenv';

config();

const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY as string;

if (!ALCHEMY_API_KEY) {
  throw new Error('ALCHEMY_API_KEY environment variable is required');
}

/**
 * Primary Alchemy provider for Ethereum mainnet
 * Used by all blockchain operations (bidding, listing, approvals, etc.)
 */
export const mainProvider = new JsonRpcProvider(
  `https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}`
);

/**
 * Fallback provider using PublicNode
 * Used for high-reliability operations when Alchemy is unavailable
 */
export const fallbackProvider = new JsonRpcProvider(
  'https://ethereum.publicnode.com'
);

/**
 * Get a provider with automatic fallback to PublicNode
 * Tests main provider connection first, falls back on failure
 *
 * @returns Promise<JsonRpcProvider> - Main provider or fallback
 */
export async function getProviderWithFallback(): Promise<JsonRpcProvider> {
  try {
    // Test main provider with quick network check
    await mainProvider.getNetwork();
    return mainProvider;
  } catch (error) {
    console.warn('[Provider] Main provider failed, using fallback', error);
    return fallbackProvider;
  }
}
