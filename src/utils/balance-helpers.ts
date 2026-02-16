/**
 * Balance error handling utilities
 * Functions for detecting, parsing, and handling insufficient balance errors
 */

import { LRUCache } from 'lru-cache';
import { addIfNotExists } from './lru-helpers';
import { warnLog } from "./debug-file-logger";
import { getCurrencyAddress } from './chains';

// Track logged balance errors to prevent flooding (1k items, 30 min TTL)
const loggedBalanceErrors = new LRUCache<string, boolean>({
  max: 1000,
  ttl: 60 * 1000 // 60 seconds (align with insufficientBalanceLogCache)
});

/**
 * OpenSea balance error pattern:
 * Matches: "does not have enough balance of payment asset... found: 849900000000000000, require: 851000000000000000"
 */
const OPENSEA_BALANCE_ERROR_REGEX = /does not have enough balance of payment asset.*found:\s*(\d+),\s*require:\s*(\d+)/i;

/**
 * @deprecated This function is no longer used. Marketplaces are not stopped due to balance errors anymore.
 * Balance errors are now logged and handled through normal error flow (logBidError).
 *
 * Previous behavior: Checked if marketplace was stopped due to insufficient balance.
 * New behavior: Balance errors don't stop marketplaces - they're logged as normal bid errors.
 */
export function isMarketplaceStoppedDueToBalance(
  taskStatus: any,
  taskId: string,
  marketplace: string
): boolean {
  // This function is deprecated and always returns false
  return false;
}

/**
 * @deprecated This function is no longer used. Balance errors are now logged but don't stop the marketplace.
 * Keeping for backwards compatibility but should not be called.
 *
 * Previous behavior: Stopped a marketplace due to insufficient balance.
 * New behavior: Balance errors are logged via handleInsufficientBalanceError and normal error handling continues.
 */
export async function stopMarketplaceDueToInsufficientBalance(
  taskStatus: any,
  taskId: string,
  marketplace: string,
  walletAddress: string,
  currency: string,
  currentBalance: number,
  requiredBalance: number,
  minBidPrice: number
): Promise<void> {
  // This function is deprecated and should not be called
  warnLog('BidFlow', `DEPRECATED: stopMarketplaceDueToInsufficientBalance called (should not happen) | taskId=${taskId} | marketplace=${marketplace}`, {
    taskId,
    marketplace: marketplace.toLowerCase() as 'blur' | 'opensea' | 'magiceden',
    wallet: walletAddress
  });
}

/**
 * Check if an error is an insufficient balance error and log it.
 * Does NOT stop the marketplace - just logs the error and returns false to let normal error handling continue.
 *
 * Note: If balance shows exactly 0, it's likely a bug in balanceLRU cache or API error handling,
 * not a real zero balance (which would be 0.0001... or similar).
 */
export async function handleInsufficientBalanceError(
  taskStatus: any,
  warningBids: any,
  warningMessage: any,
  error: any,
  taskId: string,
  marketplace: string,
  walletAddress: string,
  currency: string,
  minBidPrice: number,
  identifier: string
): Promise<boolean> {
  const errorMessage = error?.message || error?.response?.data?.message || String(error);

  // Check if this is a balance error
  const isBalanceError = errorMessage.toLowerCase().includes('balance') &&
    (errorMessage.toLowerCase().includes('insufficient') ||
      errorMessage.toLowerCase().includes('does not have enough'));

  if (!isBalanceError) {
    return false; // Not a balance error, let caller handle it
  }

  // Extract balance amounts from error message for debugging
  let currentBalance: number | null = null;
  let requiredBalance: number | null = null;

  const foundMatch = errorMessage.match(/found:\s*([\d.]+)/i);
  const requireMatch = errorMessage.match(/require:\s*([\d.]+)/i);

  if (foundMatch && requireMatch) {
    currentBalance = parseFloat(foundMatch[1]);
    requiredBalance = parseFloat(requireMatch[1]);
  } else {
    const availableMatch = errorMessage.match(/available[:\s]\s*([\d.]+)/i);
    const requiredMatch2 = errorMessage.match(/required[:\s]\s*([\d.]+)/i);

    if (availableMatch && requiredMatch2) {
      currentBalance = parseFloat(availableMatch[1]);
      requiredBalance = parseFloat(requiredMatch2[1]);
    }
  }

  // Log the balance error for debugging but don't stop the marketplace
  // If balance is exactly 0, it's likely a cache/API bug, not real balance
  const balanceInfo = currentBalance !== null && requiredBalance !== null
    ? `current=${currentBalance} | required=${requiredBalance}${currentBalance === 0 ? ' | SUSPICIOUS: balance=0 may indicate cache/API bug' : ''}`
    : 'could not parse balance amounts';

  warnLog('BidFlow', `Balance error (not stopping marketplace) | taskId=${taskId} | marketplace=${marketplace} | identifier=${identifier} | ${balanceInfo} | error=${errorMessage}`, {
    taskId,
    marketplace: marketplace.toLowerCase() as 'blur' | 'opensea' | 'magiceden',
    identifier
  });

  // Return false to let normal error handling continue (will call logBidError)
  return false;
}

/**
 * Log a balance error only once per task/marketplace to prevent log flooding.
 * Returns true if this is the first occurrence (should be logged), false if already logged.
 */
export function shouldLogBalanceError(taskId: string, marketplace: string): boolean {
  const key = `balance-${taskId}-${marketplace.toLowerCase()}`;
  return addIfNotExists(loggedBalanceErrors, key, true);
}

/**
 * Parse OpenSea balance error message to extract found/required amounts
 */
export function parseOpenseaBalanceError(errorMessage: string): { found: bigint; required: bigint; currency: string } | null {
  const match = errorMessage.match(OPENSEA_BALANCE_ERROR_REGEX);
  if (match) {
    // Extract currency from the error message (look for WETH/BETH in the message)
    let currency = 'WETH'; // Default to WETH since most errors are WETH-related
    if (errorMessage.toLowerCase().includes('beth') || errorMessage.includes('0x0000000000A39bb272e79075ade125fd351887Ac')) {
      currency = 'BETH';
    } else if (errorMessage.toLowerCase().includes('usdc') || errorMessage.includes('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')) {
      currency = 'USDC';
    }

    return {
      found: BigInt(match[1]),
      required: BigInt(match[2]),
      currency
    };
  }
  return null;
}

/**
 * Get marketplaces that use a specific currency.
 * When a balance error is detected for a currency, all marketplaces using that currency should be stopped.
 */
export function getMarketplacesForCurrency(currency: string): ('opensea' | 'magiceden' | 'blur')[] {
  switch (currency.toUpperCase()) {
    case 'WETH':
      return ['opensea', 'magiceden']; // Both OpenSea and MagicEden use WETH
    case 'BETH':
      return ['blur']; // Only Blur uses BETH (Blur Pool)
    case 'USDC':
      return ['magiceden']; // MagicEden supports USDC bids
    default:
      return [];
  }
}

/**
 * Get the token address for a currency symbol
 */
export function getTokenAddressForCurrency(currency: string): string | null {
  switch (currency.toUpperCase()) {
    case 'WETH':
      return '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2';
    case 'BETH':
      return '0x0000000000A39bb272e79075ade125fd351887Ac';
    case 'USDC':
      return '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48';
    default:
      return null;
  }
}

/**
 * Get logged balance errors cache for debugging/monitoring
 * @internal - For use by lru-cache-sampler.ts only
 */
export function getLoggedBalanceErrors(): LRUCache<string, boolean> {
  return loggedBalanceErrors;
}

/**
 * Check wallet balance before placing bid to avoid wasted API calls.
 * Uses cached balance (60s TTL) for fast pre-check with chain-aware balance fetching.
 *
 * @param taskId - Task ID for logging
 * @param walletAddress - Wallet address to check
 * @param requiredAmountEth - Required amount in ETH
 * @param currency - Currency contract address
 * @param currencySymbol - Currency symbol (WETH, BETH, etc)
 * @param slug - Collection slug for logging
 * @param marketplace - Marketplace name for logging
 * @param chain - Chain to check balance on (ethereum, base, megaeth, abstract, etc.)
 * @returns Object with sufficient boolean and actual balance
 */
export async function checkBalanceBeforeBid(
  taskId: string,
  walletAddress: string,
  requiredAmountEth: number,
  currency: string,  // Legacy parameter (might be wrong for non-Ethereum chains)
  currencySymbol: string,  // Use this to resolve correct address
  slug: string,
  marketplace: 'opensea' | 'magiceden' | 'blur',
  chain: string
): Promise<{ sufficient: boolean; balance: number }> {
  const { fetchERC20Balance } = await import('./balance');

  // Resolve correct token address for the chain
  // Don't trust the 'currency' parameter - it might be Ethereum-specific
  const tokenAddress = getCurrencyAddress(chain as any, currencySymbol);

  if (!tokenAddress) {
    warnLog('BidFlow',
      `[Balance Pre-Check] No token address found for ${currencySymbol} on chain ${chain}`,
      { taskId, marketplace, slug, chain, currencySymbol }
    );
    return { sufficient: false, balance: 0 };
  }

  const balance = await fetchERC20Balance(walletAddress, tokenAddress, chain as any);

  const sufficient = balance >= requiredAmountEth;

  if (!sufficient) {
    warnLog('BidFlow',
      `[Balance Pre-Check] Insufficient ${currencySymbol} | taskId=${taskId} | collection=${slug} | chain=${chain} | required=${requiredAmountEth} | available=${balance}`,
      { taskId, marketplace, slug, chain, required: requiredAmountEth, available: balance }
    );
  }

  return { sufficient, balance };
}
