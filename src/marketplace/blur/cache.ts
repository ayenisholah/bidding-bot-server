import { LRUCache } from 'lru-cache';

// =============================================================================
// BLUR MARKETPLACE LRU CACHES
// =============================================================================
// Migrated from Redis to LRU for performance (10-50ms faster per operation)
// All caches use lowercase wallet addresses as keys for consistency
// =============================================================================

/**
 * Blur Access Token Cache
 *
 * Stores OAuth access tokens for Blur API authentication
 * - Key: lowercase wallet address
 * - Value: JWT access token string
 * - TTL: 10 minutes (matches Blur token expiration)
 * - Max: 1000 entries (~100KB memory)
 *
 * Invalidation: Cleared immediately after successful bid placement
 */
export const blurAccessTokenLRU = new LRUCache<string, string>({
  max: 1000,
  ttl: 10 * 60 * 1000, // 10 minutes
  allowStale: false,
  updateAgeOnGet: false
});

/**
 * Auth Block Entry
 *
 * Tracks temporary authentication blocks for rate limiting
 * Uses timestamp-based expiration (dynamic TTL pattern)
 */
interface AuthBlock {
  blockedUntil: number; // Unix timestamp in milliseconds
}

/**
 * Blur Auth Block Cache
 *
 * Tracks wallets temporarily blocked due to rate limiting or auth failures
 * - Key: lowercase wallet address
 * - Value: AuthBlock with expiration timestamp
 * - TTL: 1 hour max (LRU cleanup), actual expiration checked via timestamp
 * - Max: 1000 entries (~50KB memory)
 *
 * Dynamic TTL: Expiration ranges from 60s to 3600s based on failure severity
 * Manual expiration check required on read (see pattern below)
 *
 * Usage pattern:
 * ```typescript
 * const block = blurAuthBlockLRU.get(walletAddress);
 * if (block && block.blockedUntil > Date.now()) {
 *   // Still blocked
 * } else {
 *   // Expired or doesn't exist
 *   blurAuthBlockLRU.delete(walletAddress);
 * }
 * ```
 */
export const blurAuthBlockLRU = new LRUCache<string, AuthBlock>({
  max: 1000,
  ttl: 3600 * 1000, // 1 hour max (safety cleanup)
  allowStale: false,
  updateAgeOnGet: false
});

/**
 * Wallet Pause Entry
 *
 * Tracks temporary wallet pauses for backoff/cooldown periods
 * Uses timestamp-based expiration (dynamic TTL pattern)
 */
interface WalletPause {
  pauseUntil: number; // Unix timestamp in milliseconds
}

/**
 * Blur Wallet Pause Cache
 *
 * Tracks wallets temporarily paused for backoff/cooldown
 * - Key: lowercase wallet address
 * - Value: WalletPause with expiration timestamp
 * - TTL: 5 minutes max (LRU cleanup), actual expiration checked via timestamp
 * - Max: 1000 entries (~50KB memory)
 * - CRITICAL PATH: Checked before every Blur job creation
 *
 * Dynamic TTL: Expiration ranges from 60s to 300s based on pause reason
 * Manual expiration check required on read (see pattern below)
 *
 * Usage pattern:
 * ```typescript
 * const pause = blurWalletPauseLRU.get(walletAddress);
 * if (pause && pause.pauseUntil > Date.now()) {
 *   // Still paused
 * } else {
 *   // Expired or doesn't exist
 *   blurWalletPauseLRU.delete(walletAddress);
 * }
 * ```
 */
export const blurWalletPauseLRU = new LRUCache<string, WalletPause>({
  max: 1000,
  ttl: 5 * 60 * 1000, // 5 minutes max (safety cleanup)
  allowStale: false,
  updateAgeOnGet: false
});

/**
 * Blur Collection Stats Cache
 *
 * Stores floor prices for Blur collections
 * - Key: collection slug (lowercase)
 * - Value: floor price in ETH (number)
 * - TTL: 30 seconds (fast-moving market data)
 * - Max: 1000 entries (~8KB memory)
 *
 * AllowStale: true (gracefully degrade during Blur API outages)
 */
export const blurStatsLRU = new LRUCache<string, number>({
  max: 1000,
  ttl: 30 * 1000, // 30 seconds
  allowStale: true, // Serve stale data during outages
  updateAgeOnGet: false
});

/**
 * Cache Statistics Helper
 *
 * Returns current size and capacity for all Blur caches
 * Used for monitoring and debugging
 */
export function trackBlurCacheStats() {
  return {
    accessToken: {
      size: blurAccessTokenLRU.size,
      max: 1000,
      memoryEstimate: '~100KB'
    },
    authBlock: {
      size: blurAuthBlockLRU.size,
      max: 1000,
      memoryEstimate: '~50KB'
    },
    walletPause: {
      size: blurWalletPauseLRU.size,
      max: 1000,
      memoryEstimate: '~50KB'
    },
    stats: {
      size: blurStatsLRU.size,
      max: 1000,
      memoryEstimate: '~8KB'
    },
    totalMemoryEstimate: '~208KB'
  };
}
