/**
 * Balance error handling utilities
 * Functions for detecting, parsing, and handling insufficient balance errors
 */

import { LRUCache } from 'lru-cache';
import { addIfNotExists } from './lru-helpers';
import { warnLog, debugLog } from "./debug-file-logger";

// Track logged balance errors to prevent flooding (1k items, 30 min TTL)
const loggedBalanceErrors = new LRUCache<string, boolean>({
  max: 1000,
  ttl: 30 * 60 * 1000 // 30 minutes
});

/**
 * OpenSea balance error pattern:
 * Matches: "does not have enough balance of payment asset... found: 849900000000000000, require: 851000000000000000"
 */
const OPENSEA_BALANCE_ERROR_REGEX = /does not have enough balance of payment asset.*found:\s*(\d+),\s*require:\s*(\d+)/i;

/**
 * Check if a marketplace is stopped due to insufficient balance
 */
export function isMarketplaceStoppedDueToBalance(
  taskStatus: any,
  taskId: string,
  marketplace: string
): boolean {
  const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
  const mpStatus = taskStatus[taskId]?.marketplaces?.[mpKey];
  return mpStatus?.insufficientBalance === true;
}

/**
 * Stop a marketplace due to insufficient balance.
 * Marks the marketplace as stopped and invalidates the balance cache.
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
  const mpKey = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';

  // Initialize marketplace status if it doesn't exist
  if (!taskStatus[taskId]) {
    taskStatus[taskId] = {
      selectedMarketplaces: [],
      marketplaces: {}
    };
  }

  if (!taskStatus[taskId].marketplaces[mpKey]) {
    taskStatus[taskId].marketplaces[mpKey] = {
      status: 'STOPPED',
      runningPercentage: 0,
      cancellationPercentage: 0,
      processingPercentage: 0,
      totalJobs: 0,
      completedJobs: 0,
      lastUpdate: Date.now()
    };
  }

  // Mark marketplace as stopped due to insufficient balance
  taskStatus[taskId].marketplaces[mpKey]!.insufficientBalance = true;
  taskStatus[taskId].marketplaces[mpKey]!.status = 'STOPPED';
  taskStatus[taskId].marketplaces[mpKey]!.lastUpdate = Date.now();

  // Invalidate balance cache to force fresh fetch next time
  const { invalidateBalanceCache } = await import('./balance');
  await invalidateBalanceCache(walletAddress, currency);

  warnLog('BidFlow', `STOP MARKETPLACE | taskId=${taskId} | marketplace=${marketplace} | balance=${currentBalance} < minBid=${minBidPrice} | required=${requiredBalance}`, {
    taskId,
    marketplace: marketplace.toLowerCase() as 'blur' | 'opensea' | 'magiceden',
    wallet: walletAddress
  });
}

/**
 * Check if an error is an insufficient balance error and handle accordingly.
 * Returns true if the error was handled (marketplace stopped or bid skipped), false otherwise.
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

  // Extract balance amounts from error message
  // Pattern 1: "found: X, require: Y" (OpenSea)
  // Pattern 2: "available: X WETH" and "required: Y WETH"
  let currentBalance: number | null = null;
  let requiredBalance: number | null = null;

  const foundMatch = errorMessage.match(/found:\s*([\d.]+)/i);
  const requireMatch = errorMessage.match(/require:\s*([\d.]+)/i);

  if (foundMatch && requireMatch) {
    currentBalance = parseFloat(foundMatch[1]);
    requiredBalance = parseFloat(requireMatch[1]);
  } else {
    // Match both "available: X" and "available X" formats (colon OR space)
    const availableMatch = errorMessage.match(/available[:\s]\s*([\d.]+)/i);
    const requiredMatch2 = errorMessage.match(/required[:\s]\s*([\d.]+)/i);

    if (availableMatch && requiredMatch2) {
      currentBalance = parseFloat(availableMatch[1]);
      requiredBalance = parseFloat(requiredMatch2[1]);
    }
  }

  // Consolidated insufficient balance logging (previously 3 separate logs)
  if (currentBalance !== null && requiredBalance !== null) {
    // Decision: Stop marketplace if balance < min bid price, otherwise skip this bid
    if (currentBalance < minBidPrice) {
      await stopMarketplaceDueToInsufficientBalance(
        taskStatus,
        taskId,
        marketplace,
        walletAddress,
        currency,
        currentBalance,
        requiredBalance,
        minBidPrice
      );
      return true;
    } else {
      // Single consolidated log for insufficient balance skip (was 3 logs)
      debugLog('BidFlow', `SKIP BID | taskId=${taskId} | marketplace=${marketplace} | identifier=${identifier} | insufficient_balance | current=${currentBalance} | required=${requiredBalance}`, {
        taskId,
        marketplace: marketplace.toLowerCase() as 'blur' | 'opensea' | 'magiceden',
        identifier
      });
      return true; // Skip this bid, continue processing other tokens
    }
  } else {
    // Couldn't parse balance amounts - set warning instead of stopping
    // Only stop if we're SURE balance < minBid, which we can't confirm here
    warnLog('BidFlow', `Balance error detected but couldn't parse amounts - setting warning | taskId=${taskId} | marketplace=${marketplace} | error=${errorMessage}`, {
      taskId,
      marketplace: marketplace.toLowerCase() as 'blur' | 'opensea' | 'magiceden',
      identifier
    });

    // Set warning flags instead of stopping marketplace
    if (!warningBids[taskId]) {
      warningBids[taskId] = { opensea: false, magiceden: false, blur: false };
    }
    if (!warningMessage[taskId]) {
      warningMessage[taskId] = { opensea: "", magiceden: "", blur: "" };
    }
    const mpLower = marketplace.toLowerCase() as 'opensea' | 'magiceden' | 'blur';
    warningBids[taskId][mpLower] = true;
    warningMessage[taskId][mpLower] = `Insufficient balance for some bids (couldn't parse exact amounts from error)`;

    return true; // Skip this bid but don't stop marketplace
  }
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
