/**
 * Custom error classes for marketplace-specific errors
 */

export class MarketplaceError extends Error {
  code?: string;
  marketplace: string;
  statusCode?: number;
  
  constructor(message: string, marketplace: string, code?: string, statusCode?: number) {
    super(message);
    this.name = 'MarketplaceError';
    this.marketplace = marketplace;
    this.code = code;
    this.statusCode = statusCode;
  }
}

export class InsufficientBalanceError extends MarketplaceError {
  requiredAmount: number;
  availableAmount: number;
  currency: string;
  
  constructor(
    marketplace: string, 
    currency: string, 
    requiredAmount: number, 
    availableAmount: number,
    walletAddress: string,
    collection: string
  ) {
    const message = `Insufficient ${currency} balance: required ${requiredAmount} ${currency}, available ${availableAmount} ${currency} for wallet ${walletAddress} on collection ${collection}`;
    super(message, marketplace, 'INSUFFICIENT_BALANCE');
    this.name = 'InsufficientBalanceError';
    this.requiredAmount = requiredAmount;
    this.availableAmount = availableAmount;
    this.currency = currency;
  }
}

export class TransferRestrictedError extends MarketplaceError {
  contractAddress: string;
  
  constructor(marketplace: string, contractAddress: string, cached = false) {
    const message = `${marketplace}: Collection ${contractAddress} restricts transfers - skipping bid${cached ? ' (cached)' : ''}`;
    super(message, marketplace, 'TRANSFER_RESTRICTED');
    this.name = 'TransferRestrictedError';
    this.contractAddress = contractAddress;
  }
}

export class MinimumPriceError extends MarketplaceError {
  bidAmount: number;
  minimumAmount: number;
  currency: string;
  
  constructor(
    marketplace: string,
    currency: string,
    bidAmount: number,
    minimumAmount: number,
    collection: string
  ) {
    const message = `Offer price ${bidAmount} ${currency} is below minimum ${marketplace} offer price (${minimumAmount} ${currency}) for collection ${collection}`;
    super(message, marketplace, 'BELOW_MINIMUM_PRICE');
    this.name = 'MinimumPriceError';
    this.bidAmount = bidAmount;
    this.minimumAmount = minimumAmount;
    this.currency = currency;
  }
}

export class AuthenticationError extends MarketplaceError {
  constructor(marketplace: string, message: string) {
    super(message, marketplace, 'AUTHENTICATION_ERROR');
    this.name = 'AuthenticationError';
  }
}

export class RateLimitError extends MarketplaceError {
  retryAfter?: number;
  
  constructor(marketplace: string, message: string, retryAfter?: number) {
    super(message, marketplace, 'RATE_LIMIT_ERROR');
    this.name = 'RateLimitError';
    this.retryAfter = retryAfter;
  }
}

export class InvalidRequestError extends MarketplaceError {
  constructor(marketplace: string, message: string, statusCode?: number) {
    super(message, marketplace, 'INVALID_REQUEST', statusCode);
    this.name = 'InvalidRequestError';
  }
}

/**
 * Check if an error should be treated as a "soft" error that doesn't increment error stats
 */
export function isSoftError(error: any): boolean {
  // Transfer restricted errors should be counted in errorStats per user request
  return error instanceof MinimumPriceError ||
         (error instanceof InsufficientBalanceError && error.code === 'INSUFFICIENT_BALANCE');
}

/**
 * Check if an error should be retried
 */
export function isRetryableError(error: any): boolean {
  return error instanceof RateLimitError ||
         (error instanceof MarketplaceError && error.statusCode !== undefined && error.statusCode >= 500);
}